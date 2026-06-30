// SPDX-License-Identifier: MIT OR Apache-2.0
//! The embedded ring-buffer history source (#370): in-memory time-series history WITHOUT an
//! external Prometheus, behind the SAME pluggable [`crate::history::HistorySource`] interface as the
//! Prometheus adapter (#356). A standalone / OSS deployment (no Prometheus, or a node whose
//! `/metrics` is locked to the Prometheus CIDR) still renders short-window trend panels from the
//! samples the console's own poll loop already collects.
//!
//! ## Shape
//!
//! [`EmbeddedHistory`] is a bounded `(metric, node) -> ring of (unix_ts, value)` store. The poll loop
//! [`EmbeddedHistory::record`]s the headline figures each tick; the store self-PRUNES on every record
//! so memory is bounded TWO ways: by AGE (a retention window) and by a per-series POINT CAP (a burst
//! of fast polls cannot grow a series past the cap). [`EmbeddedSource`] wraps an
//! `Arc<EmbeddedHistory>` and serves [`crate::history::HistorySource::query_range`] from it, applying
//! the SAME `is_allowed_metric` allowlist the Prometheus source does (a query never names anything
//! but a bare `ironcache_*` metric). A bare metric may have several series (one per node), exactly
//! like the Prometheus matrix shape.
//!
//! ## Resolution
//!
//! `step_secs` is honored BEST-EFFORT: the embedded store keeps the raw poll-cadence samples and
//! returns every retained point inside `[start, end]` (the UI down-samples for display). It does not
//! re-bucket to an arbitrary step; the poll interval IS the native resolution.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use crate::history::{HistoryError, HistorySource, TimeSeries, is_allowed_metric};

/// The default per-series point cap (a defensive second bound beside the retention window): at a
/// typical poll cadence this is many hours of samples, and it caps a misconfigured fast-poll burst.
const DEFAULT_MAX_POINTS_PER_SERIES: usize = 4096;

/// A series identity in the embedded store: a bare metric name plus the node it was sampled from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SeriesKey {
    metric: String,
    node: String,
}

/// A bounded in-memory time-series store: `(metric, node) -> ring of (unix_ts, value)`, pruned on
/// every record by both a retention window and a per-series point cap. Shared by `Arc` between the
/// poll loop (which records) and the [`EmbeddedSource`] (which queries).
#[derive(Debug)]
pub struct EmbeddedHistory {
    series: Mutex<HashMap<SeriesKey, VecDeque<(u64, f64)>>>,
    retention_secs: u64,
    max_points_per_series: usize,
}

impl EmbeddedHistory {
    /// A store retaining `retention` of history per `(metric, node)` series.
    #[must_use]
    pub fn new(retention: Duration) -> EmbeddedHistory {
        EmbeddedHistory {
            series: Mutex::new(HashMap::new()),
            retention_secs: retention.as_secs().max(1),
            max_points_per_series: DEFAULT_MAX_POINTS_PER_SERIES,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SeriesKey, VecDeque<(u64, f64)>>> {
        self.series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record one sample of `metric` for `node` at `unix_ts`. The series self-prunes: points older
    /// than `now - retention` are dropped (using `unix_ts` as the clock so the caller's env-seam time
    /// drives retention, ADR-0003), and the oldest points are evicted past the cap. A non-finite
    /// value (NaN/inf) is ignored so a parse glitch cannot poison a series.
    pub fn record(&self, metric: &str, node: &str, unix_ts: u64, value: f64) {
        if !value.is_finite() {
            return;
        }
        let key = SeriesKey {
            metric: metric.to_owned(),
            node: node.to_owned(),
        };
        let cutoff = unix_ts.saturating_sub(self.retention_secs);
        let mut g = self.lock();
        let ring = g.entry(key).or_default();
        ring.push_back((unix_ts, value));
        // Prune by age: drop points strictly older than the retention cutoff.
        while ring.front().is_some_and(|&(ts, _)| ts < cutoff) {
            ring.pop_front();
        }
        // Prune by count: keep only the newest `max_points_per_series`.
        while ring.len() > self.max_points_per_series {
            ring.pop_front();
        }
    }

    /// The retained series for `metric` whose samples fall in `[start_unix, end_unix]`, one
    /// [`TimeSeries`] per node, points in ascending time order. Labels are `__name__` + `node` (the
    /// Prometheus convention), so the UI renders them like a Prometheus result.
    fn query(&self, metric: &str, start_unix: u64, end_unix: u64) -> Vec<TimeSeries> {
        let g = self.lock();
        let mut out = Vec::new();
        for (key, ring) in g.iter() {
            if key.metric != metric {
                continue;
            }
            let points: Vec<(u64, f64)> = ring
                .iter()
                .copied()
                .filter(|&(ts, _)| ts >= start_unix && ts <= end_unix)
                .collect();
            if points.is_empty() {
                continue;
            }
            let mut labels = BTreeMap::new();
            labels.insert("__name__".to_owned(), key.metric.clone());
            labels.insert("node".to_owned(), key.node.clone());
            out.push(TimeSeries { labels, points });
        }
        // Deterministic order (by node) so a query is reproducible.
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        out
    }
}

/// The embedded [`HistorySource`] (#370): serves `query_range` from a shared [`EmbeddedHistory`],
/// gated by the same `is_allowed_metric` allowlist as the Prometheus source. Swappable with
/// [`crate::history::PrometheusSource`] with no API change.
#[derive(Debug, Clone)]
pub struct EmbeddedSource {
    store: std::sync::Arc<EmbeddedHistory>,
}

impl EmbeddedSource {
    /// Wrap a shared store (the same `Arc` the poll loop records into).
    #[must_use]
    pub fn new(store: std::sync::Arc<EmbeddedHistory>) -> EmbeddedSource {
        EmbeddedSource { store }
    }
}

impl HistorySource for EmbeddedSource {
    fn query_range<'a>(
        &'a self,
        metric: &'a str,
        start_unix: u64,
        end_unix: u64,
        _step_secs: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<TimeSeries>, HistoryError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // Same SSRF / injection guard as the Prometheus source: a bare ironcache_* name only.
            if !is_allowed_metric(metric) {
                return Err(HistoryError::DisallowedMetric(metric.to_owned()));
            }
            Ok(self.store.query(metric, start_unix, end_unix))
        })
    }
}

/// Record the headline figures from one node's parsed `INFO` into the embedded history at `unix_ts`.
/// The metric names MIRROR the engine's `/metrics` gauge/counter names (#362) so the embedded source
/// and a Prometheus source render the SAME series; an absent INFO field is skipped. The poll loop
/// calls this once per reachable node per tick.
#[allow(clippy::cast_precision_loss)] // counts/bytes; f64's 53-bit mantissa covers the practical range, and the trend panel wants f64.
pub fn record_node_samples(
    history: &EmbeddedHistory,
    node: &str,
    info: &crate::info::NodeInfo,
    unix_ts: u64,
) {
    let put = |metric: &str, v: Option<u64>| {
        if let Some(v) = v {
            history.record(metric, node, unix_ts, v as f64);
        }
    };
    put("ironcache_used_memory_bytes", info.used_memory);
    put("ironcache_used_memory_rss_bytes", info.used_memory_rss);
    put("ironcache_connected_clients", info.connected_clients);
    put("ironcache_keyspace_keys", info.total_keys);
    put("ironcache_keyspace_hits_total", info.keyspace_hits);
    put("ironcache_keyspace_misses_total", info.keyspace_misses);
    put(
        "ironcache_commands_processed_total",
        info.total_commands_processed,
    );
    put("ironcache_evicted_keys_total", info.evicted_keys);
    put("ironcache_expired_keys_total", info.expired_keys);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<F: std::future::Future>(f: F) -> F::Output {
        // A tiny current-thread runtime so the async trait method can be driven in a sync test.
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn records_and_queries_one_series_per_node() {
        let h = EmbeddedHistory::new(Duration::from_secs(3600));
        h.record("ironcache_used_memory_bytes", "10.0.0.1:6379", 100, 1000.0);
        h.record("ironcache_used_memory_bytes", "10.0.0.1:6379", 110, 1100.0);
        h.record("ironcache_used_memory_bytes", "10.0.0.2:6379", 105, 2000.0);
        let series = h.query("ironcache_used_memory_bytes", 0, 1000);
        assert_eq!(series.len(), 2, "one series per node");
        // Sorted by labels: node 10.0.0.1 first.
        assert_eq!(series[0].labels["node"], "10.0.0.1:6379");
        assert_eq!(series[0].points, vec![(100, 1000.0), (110, 1100.0)]);
        assert_eq!(series[1].labels["node"], "10.0.0.2:6379");
        assert_eq!(series[1].points, vec![(105, 2000.0)]);
    }

    #[test]
    fn prunes_points_older_than_the_retention_window() {
        let h = EmbeddedHistory::new(Duration::from_secs(60));
        h.record("ironcache_keyspace_keys", "n", 100, 1.0);
        h.record("ironcache_keyspace_keys", "n", 130, 2.0);
        // A record at ts=200 sets the cutoff to 200-60=140; the ts=100 and ts=130 points are dropped.
        h.record("ironcache_keyspace_keys", "n", 200, 3.0);
        let series = h.query("ironcache_keyspace_keys", 0, 10_000);
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].points,
            vec![(200, 3.0)],
            "only the in-window point remains"
        );
    }

    #[test]
    fn the_query_window_filters_points() {
        let h = EmbeddedHistory::new(Duration::from_secs(100_000));
        for ts in [10u64, 20, 30, 40, 50] {
            h.record("ironcache_commands_processed_total", "n", ts, ts as f64);
        }
        let series = h.query("ironcache_commands_processed_total", 20, 40);
        assert_eq!(series[0].points, vec![(20, 20.0), (30, 30.0), (40, 40.0)]);
    }

    #[test]
    fn a_non_finite_sample_is_ignored() {
        let h = EmbeddedHistory::new(Duration::from_secs(3600));
        h.record("ironcache_x", "n", 1, f64::NAN);
        h.record("ironcache_x", "n", 2, f64::INFINITY);
        h.record("ironcache_x", "n", 3, 5.0);
        let series = h.query("ironcache_x", 0, 10);
        assert_eq!(
            series[0].points,
            vec![(3, 5.0)],
            "only the finite sample is kept"
        );
    }

    #[test]
    fn the_per_series_point_cap_bounds_memory() {
        let mut h = EmbeddedHistory::new(Duration::from_secs(1_000_000));
        h.max_points_per_series = 3;
        for ts in 1..=10u64 {
            h.record("ironcache_y", "n", ts, ts as f64);
        }
        let series = h.query("ironcache_y", 0, 100);
        // Only the newest 3 survive the cap.
        assert_eq!(series[0].points, vec![(8, 8.0), (9, 9.0), (10, 10.0)]);
    }

    #[test]
    fn record_node_samples_maps_info_fields_to_metric_names() {
        let h = EmbeddedHistory::new(Duration::from_secs(3600));
        let info = crate::info::NodeInfo {
            used_memory: Some(12345),
            connected_clients: Some(7),
            total_keys: Some(99),
            total_commands_processed: Some(5000),
            // keyspace_hits left None: its series must be ABSENT.
            ..Default::default()
        };
        record_node_samples(&h, "10.0.0.1:6379", &info, 1000);
        assert_eq!(
            h.query("ironcache_used_memory_bytes", 0, 2000)[0].points,
            vec![(1000, 12345.0)]
        );
        assert_eq!(
            h.query("ironcache_keyspace_keys", 0, 2000)[0].points,
            vec![(1000, 99.0)]
        );
        assert_eq!(
            h.query("ironcache_commands_processed_total", 0, 2000)[0].points,
            vec![(1000, 5000.0)]
        );
        // An absent INFO field records NO series.
        assert!(h.query("ironcache_keyspace_hits_total", 0, 2000).is_empty());
    }

    #[test]
    fn the_embedded_source_enforces_the_metric_allowlist() {
        let store = std::sync::Arc::new(EmbeddedHistory::new(Duration::from_secs(60)));
        store.record("ironcache_used_memory_bytes", "n", 5, 7.0);
        let src = EmbeddedSource::new(store);
        // An allowed bare metric returns its series.
        let ok = block(src.query_range("ironcache_used_memory_bytes", 0, 100, 15)).unwrap();
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].points, vec![(5, 7.0)]);
        // A non-ironcache / injection-shaped name is rejected (the SSRF/injection guard).
        let denied = block(src.query_range("up{job=\"x\"}", 0, 100, 15));
        assert!(
            matches!(denied, Err(HistoryError::DisallowedMetric(_))),
            "{denied:?}"
        );
    }
}
