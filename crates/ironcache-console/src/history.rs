// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console history layer (issue #356): time-series history behind a pluggable
//! [`HistorySource`] adapter.
//!
//! The dashboard wants historical series (memory, hit ratio, ops over time), not
//! just the latest poll. v1 sources that from the SAME Prometheus the engine
//! already exports to, behind a small trait so the source is swappable: an
//! embedded ring-buffer source (#370) can implement [`HistorySource`] later
//! without touching the API layer that consumes it.
//!
//! [`PrometheusSource`] is the v1 adapter. It queries Prometheus's
//! `query_range` HTTP API through the hand-rolled [`crate::httpclient`] (bounded:
//! connect + read timeouts, a response-size cap, never hangs) and maps the
//! `matrix` result into [`TimeSeries`].
//!
//! ## SECURITY (SSRF + PromQL injection)
//!
//! Two boundaries the caller MUST respect (enforced at the API edge, see
//! [`crate::api`]):
//!
//! * The Prometheus base URL comes ONLY from server config, never from request
//!   input, so a request cannot point the console at an arbitrary host (SSRF).
//! * The `metric` is an ALLOWLISTED bare metric name ([`is_allowed_metric`]); the
//!   adapter builds the PromQL query string itself and URL-encodes it, so a
//!   request cannot inject raw PromQL / a `query=` of its choosing. This module
//!   also re-validates the metric defensively before issuing the query.
//!
//! ## Determinism (ADR-0003)
//!
//! No clock and no RNG here: the start/end time window is passed IN by the caller
//! (the API reads it through the `ironcache-env` seam); this layer is pure I/O +
//! parsing.

use std::time::Duration;

use crate::httpclient::{self, HttpError};

/// One labeled time series: the metric's label set plus its `(unix_ts, value)`
/// samples in time order. Serialized to JSON for the `/api/timeseries` response.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TimeSeries {
    /// The metric's label set (Prometheus `metric` object: name + labels).
    pub labels: std::collections::BTreeMap<String, String>,
    /// The samples: `(unix_timestamp_seconds, value)` in ascending time order.
    pub points: Vec<(u64, f64)>,
}

/// A typed error from a history query. Distinct variants so the API can map a
/// transport failure, a source-reported error, and a parse fault to the right
/// status / message.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// No history source is configured (no `prometheus_url`). The API maps this to
    /// a `503` ("no history source configured").
    #[error("no history source configured")]
    NotConfigured,
    /// The requested metric is not on the allowlist (SSRF / injection guard). The
    /// API maps this to a `400`.
    #[error("metric '{0}' is not an allowed ironcache metric name")]
    DisallowedMetric(String),
    /// The underlying HTTP transport failed (connect/timeout/IO/too-large).
    #[error("history transport error: {0}")]
    Transport(#[from] HttpError),
    /// Prometheus answered with a non-success HTTP status or a
    /// `{"status":"error"}` body. The message is Prometheus's error text.
    #[error("history source error: {0}")]
    Source(String),
    /// The Prometheus response could not be parsed as the expected matrix JSON.
    #[error("history response parse error: {0}")]
    Parse(String),
}

/// The pluggable history source. ASYNC: the implementation does I/O (an HTTP
/// query, or later a lock-free ring-buffer read). The trait is the seam that lets
/// the embedded source (#370) drop in behind the same API.
///
/// `start_unix` / `end_unix` are an inclusive Unix-seconds window; `step_secs` is
/// the resolution. The implementation returns one [`TimeSeries`] per matching
/// series (a single bare metric may still have several label combinations).
pub trait HistorySource: Send + Sync {
    /// Query the historical samples for `metric` over `[start_unix, end_unix]` at
    /// `step_secs` resolution.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on a disallowed metric, a transport failure, a
    /// source-reported error, or an unparseable response.
    fn query_range<'a>(
        &'a self,
        metric: &'a str,
        start_unix: u64,
        end_unix: u64,
        step_secs: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<TimeSeries>, HistoryError>> + Send + 'a>,
    >;
}

/// Whether `metric` is an allowed bare IronCache metric name. The allowlist is the
/// SSRF / PromQL-injection guard: only `ironcache_*` and `ironcache_console_*`
/// series are queryable, and only as a BARE metric name (letters, digits, and
/// `_`), never raw PromQL. A name with a `(`, `{`, space, or any other character
/// is rejected, so a request cannot smuggle a function call, a label matcher, or a
/// second `query=` parameter through the metric field.
#[must_use]
pub fn is_allowed_metric(metric: &str) -> bool {
    // Must start with the ironcache prefix (covers ironcache_console_* too) AND
    // carry a non-empty suffix after it, so the bare prefix `ironcache_` (not a
    // real metric) is rejected.
    let Some(suffix) = metric.strip_prefix("ironcache_") else {
        return false;
    };
    if suffix.is_empty() {
        return false;
    }
    // A Prometheus metric name is [a-zA-Z_:][a-zA-Z0-9_:]*. We are STRICTER: we
    // forbid ':' as well (no recording-rule / namespaced names are exposed), and
    // require the whole string to be [a-z A-Z 0-9 _]. This is an allowlist, not a
    // denylist, so any injection character (`(`, `{`, space, `&`, ...) is out.
    metric
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The Prometheus-backed [`HistorySource`] (v1). Holds the configured base URL and
/// the timeout bounds; each query is a single bounded HTTP GET.
#[derive(Debug, Clone)]
pub struct PrometheusSource {
    /// The Prometheus base URL (e.g. `http://prometheus:9090`), from SERVER config
    /// only. Never request input (SSRF boundary).
    base_url: String,
    /// TCP connect timeout for a query.
    connect_timeout: Duration,
    /// Response read timeout for a query (the hang guard).
    read_timeout: Duration,
}

impl PrometheusSource {
    /// Construct from the configured base URL and the timeout bounds. The trailing
    /// `/` (if any) is trimmed so the path join is clean.
    #[must_use]
    pub fn new(base_url: &str, connect_timeout: Duration, read_timeout: Duration) -> Self {
        PrometheusSource {
            base_url: base_url.trim_end_matches('/').to_owned(),
            connect_timeout,
            read_timeout,
        }
    }

    /// Build the `query_range` request URL. The `metric` has ALREADY been
    /// allowlist-validated by the caller; it is still URL-encoded here as the value
    /// of the `query` parameter (defense in depth). The console constructs the
    /// PromQL itself (just the bare metric selector), so no raw PromQL crosses the
    /// boundary.
    fn build_url(&self, metric: &str, start_unix: u64, end_unix: u64, step_secs: u64) -> String {
        // step must be at least 1s (Prometheus rejects step=0); the caller bounds
        // it, but clamp defensively.
        let step = step_secs.max(1);
        format!(
            "{}/api/v1/query_range?query={}&start={}&end={}&step={}",
            self.base_url,
            url_encode(metric),
            start_unix,
            end_unix,
            step,
        )
    }
}

impl HistorySource for PrometheusSource {
    fn query_range<'a>(
        &'a self,
        metric: &'a str,
        start_unix: u64,
        end_unix: u64,
        step_secs: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<TimeSeries>, HistoryError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // Defense in depth: re-validate the metric even though the API edge
            // already did. A disallowed name never reaches the network.
            if !is_allowed_metric(metric) {
                return Err(HistoryError::DisallowedMetric(metric.to_owned()));
            }
            let url = self.build_url(metric, start_unix, end_unix, step_secs);
            let resp = httpclient::get(&url, self.connect_timeout, self.read_timeout).await?;
            // A 2xx carries the matrix (or a status:error body); a non-2xx is a
            // source error. Prometheus returns 400/422/503 with a JSON error body
            // on a bad query; surface its text when we can parse it.
            if !(200..300).contains(&resp.status) {
                let msg = extract_error(&resp.body)
                    .unwrap_or_else(|| format!("Prometheus HTTP {}", resp.status));
                return Err(HistoryError::Source(msg));
            }
            parse_query_range(&resp.body)
        })
    }
}

/// Parse a Prometheus `query_range` response body into [`TimeSeries`] values.
///
/// Expects `{"status":"success","data":{"resultType":"matrix","result":[...]}}`.
/// A `{"status":"error",...}` body is a [`HistoryError::Source`]. A non-matrix
/// result type, or a malformed sample, is a [`HistoryError::Parse`]. No panics: a
/// missing/odd field is mapped to a typed error, never an index/unwrap.
///
/// # Errors
///
/// Returns [`HistoryError::Source`] for a status-error body and
/// [`HistoryError::Parse`] for malformed / unexpected JSON.
pub fn parse_query_range(body: &[u8]) -> Result<Vec<TimeSeries>, HistoryError> {
    let root: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| HistoryError::Parse(format!("invalid JSON: {e}")))?;

    let status = root.get("status").and_then(serde_json::Value::as_str);
    match status {
        Some("success") => {}
        Some("error") => {
            let msg = root
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Prometheus reported an error");
            return Err(HistoryError::Source(msg.to_owned()));
        }
        other => {
            return Err(HistoryError::Parse(format!(
                "unexpected status field: {other:?}"
            )));
        }
    }

    let data = root
        .get("data")
        .ok_or_else(|| HistoryError::Parse("missing data field".to_owned()))?;
    let result_type = data.get("resultType").and_then(serde_json::Value::as_str);
    if result_type != Some("matrix") {
        return Err(HistoryError::Parse(format!(
            "expected resultType=matrix, got {result_type:?}"
        )));
    }
    let result = data
        .get("result")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| HistoryError::Parse("data.result is not an array".to_owned()))?;

    let mut series = Vec::with_capacity(result.len());
    for entry in result {
        series.push(parse_series(entry)?);
    }
    Ok(series)
}

/// Parse one matrix series object: `{"metric":{...},"values":[[ts,"val"],...]}`.
fn parse_series(entry: &serde_json::Value) -> Result<TimeSeries, HistoryError> {
    let mut labels = std::collections::BTreeMap::new();
    if let Some(metric_obj) = entry.get("metric").and_then(serde_json::Value::as_object) {
        for (k, v) in metric_obj {
            // Label values are always strings in Prometheus JSON; a non-string is
            // skipped rather than failing the whole series.
            if let Some(s) = v.as_str() {
                labels.insert(k.clone(), s.to_owned());
            }
        }
    }
    let values = entry
        .get("values")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| HistoryError::Parse("series has no values array".to_owned()))?;
    let mut points = Vec::with_capacity(values.len());
    for sample in values {
        points.push(parse_sample(sample)?);
    }
    Ok(TimeSeries { labels, points })
}

/// Parse one sample `[<unix_ts_number>, "<value_string>"]` into `(u64, f64)`. The
/// timestamp is a JSON number (seconds, possibly fractional); the value is a
/// STRING per the Prometheus wire format. A malformed sample is a parse error.
fn parse_sample(sample: &serde_json::Value) -> Result<(u64, f64), HistoryError> {
    let arr = sample
        .as_array()
        .ok_or_else(|| HistoryError::Parse("sample is not a [ts, value] array".to_owned()))?;
    if arr.len() != 2 {
        return Err(HistoryError::Parse(format!(
            "sample must have 2 elements, got {}",
            arr.len()
        )));
    }
    let ts_f = arr[0]
        .as_f64()
        .ok_or_else(|| HistoryError::Parse("sample timestamp is not a number".to_owned()))?;
    // Timestamps are non-negative Unix seconds; truncate the fractional part. A
    // negative timestamp clamps to 0 (cannot happen in practice).
    let ts = if ts_f < 0.0 { 0 } else { ts_f as u64 };
    // The value is a string (Prometheus encodes it as text to preserve NaN/Inf).
    let val_str = arr[1]
        .as_str()
        .ok_or_else(|| HistoryError::Parse("sample value is not a string".to_owned()))?;
    let val = parse_prom_float(val_str)?;
    Ok((ts, val))
}

/// Parse a Prometheus float value string, handling the special tokens it uses for
/// non-finite values (`NaN`, `+Inf`, `-Inf`, `Inf`).
fn parse_prom_float(s: &str) -> Result<f64, HistoryError> {
    match s {
        "NaN" => Ok(f64::NAN),
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        other => other
            .parse::<f64>()
            .map_err(|e| HistoryError::Parse(format!("invalid sample value '{other}': {e}"))),
    }
}

/// Pull a Prometheus error message out of a (possibly JSON) error body, for the
/// non-2xx path. Returns `None` if the body is not the expected JSON error shape.
fn extract_error(body: &[u8]) -> Option<String> {
    let root: serde_json::Value = serde_json::from_slice(body).ok()?;
    root.get("error")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

/// Percent-encode a string for use as a URL query VALUE (RFC 3986). Unreserved
/// characters (`A-Z a-z 0-9 - _ . ~`) pass through; everything else is `%XX`.
/// Sufficient and correct for the bare metric names we pass (which the allowlist
/// already restricts to `[A-Za-z0-9_]`), and conservative for any future value.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_digit(b >> 4));
            out.push(hex_digit(b & 0x0f));
        }
    }
    out
}

/// The uppercase hex digit for a nibble (0..=15).
fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + (n - 10)) as char,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_accepts_ironcache_metrics() {
        assert!(is_allowed_metric("ironcache_used_memory_bytes"));
        assert!(is_allowed_metric("ironcache_console_uptime_seconds"));
        assert!(is_allowed_metric("ironcache_keyspace_hits_total"));
    }

    #[test]
    fn allowlist_rejects_non_ironcache_and_injection() {
        // Wrong prefix.
        assert!(!is_allowed_metric("node_cpu_seconds_total"));
        assert!(!is_allowed_metric("up"));
        // Injection attempts: function calls, label matchers, a second query
        // parameter, whitespace, a colon (recording-rule namespace).
        assert!(!is_allowed_metric("ironcache_x{job=\"y\"}"));
        assert!(!is_allowed_metric("rate(ironcache_x[5m])"));
        assert!(!is_allowed_metric("ironcache_x&query=up"));
        assert!(!is_allowed_metric("ironcache_x or up"));
        assert!(!is_allowed_metric("ironcache:x"));
        assert!(!is_allowed_metric("ironcache_"));
        // Empty / bare prefix-only is allowed by the byte check but is harmless
        // (no such metric); the non-empty residue is still alnum/underscore.
        assert!(!is_allowed_metric(""));
    }

    #[test]
    fn url_encode_passes_unreserved_and_escapes_the_rest() {
        assert_eq!(url_encode("ironcache_used_memory"), "ironcache_used_memory");
        assert_eq!(url_encode("a b"), "a%20b");
        assert_eq!(url_encode("x{y}"), "x%7By%7D");
        assert_eq!(url_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(url_encode("a.b-c~d"), "a.b-c~d");
    }

    #[test]
    fn build_url_constructs_the_query_range_request() {
        let src = PrometheusSource::new(
            "http://prom:9090/",
            Duration::from_secs(2),
            Duration::from_secs(2),
        );
        let url = src.build_url("ironcache_used_memory_bytes", 1000, 2000, 15);
        assert_eq!(
            url,
            "http://prom:9090/api/v1/query_range?query=ironcache_used_memory_bytes&start=1000&end=2000&step=15"
        );
    }

    #[test]
    fn build_url_clamps_zero_step_to_one() {
        let src = PrometheusSource::new(
            "http://prom:9090",
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let url = src.build_url("ironcache_x", 0, 10, 0);
        assert!(url.ends_with("step=1"), "{url}");
    }

    #[test]
    fn parse_success_matrix_with_two_series() {
        let body = br#"{
            "status":"success",
            "data":{
                "resultType":"matrix",
                "result":[
                    {"metric":{"__name__":"ironcache_used_memory_bytes","instance":"a"},
                     "values":[[1000,"100"],[1015,"110.5"]]},
                    {"metric":{"__name__":"ironcache_used_memory_bytes","instance":"b"},
                     "values":[[1000,"200"]]}
                ]
            }
        }"#;
        let series = parse_query_range(body).unwrap();
        assert_eq!(series.len(), 2);
        assert_eq!(
            series[0].labels.get("instance").map(String::as_str),
            Some("a")
        );
        assert_eq!(series[0].points, vec![(1000, 100.0), (1015, 110.5)]);
        assert_eq!(series[1].points, vec![(1000, 200.0)]);
    }

    #[test]
    fn parse_empty_matrix_is_ok_empty() {
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#;
        let series = parse_query_range(body).unwrap();
        assert!(series.is_empty());
    }

    #[test]
    fn parse_status_error_is_source_error() {
        let body =
            br#"{"status":"error","errorType":"bad_data","error":"parse error: unexpected }"}"#;
        let err = parse_query_range(body).unwrap_err();
        match err {
            HistoryError::Source(msg) => assert!(msg.contains("parse error")),
            other => panic!("expected Source, got {other:?}"),
        }
    }

    #[test]
    fn parse_non_matrix_result_type_is_parse_error() {
        // A vector result (the instant-query shape) is not what query_range gives.
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
        assert!(matches!(
            parse_query_range(body).unwrap_err(),
            HistoryError::Parse(_)
        ));
    }

    #[test]
    fn parse_malformed_sample_is_parse_error_not_panic() {
        // A sample with one element (missing the value) must NOT panic.
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1000]]}
        ]}}"#;
        assert!(matches!(
            parse_query_range(body).unwrap_err(),
            HistoryError::Parse(_)
        ));
        // A numeric (not string) value also errors rather than panicking.
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1000,100]]}
        ]}}"#;
        assert!(matches!(
            parse_query_range(body).unwrap_err(),
            HistoryError::Parse(_)
        ));
    }

    #[test]
    fn parse_handles_special_float_tokens() {
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1,"NaN"],[2,"+Inf"],[3,"-Inf"]]}
        ]}}"#;
        let series = parse_query_range(body).unwrap();
        let pts = &series[0].points;
        assert!(pts[0].1.is_nan());
        assert!(pts[1].1.is_infinite() && pts[1].1 > 0.0);
        assert!(pts[2].1.is_infinite() && pts[2].1 < 0.0);
    }

    #[test]
    fn parse_invalid_json_is_parse_error() {
        assert!(matches!(
            parse_query_range(b"not json").unwrap_err(),
            HistoryError::Parse(_)
        ));
    }

    #[test]
    fn parse_fractional_timestamp_is_truncated() {
        let body = br#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1000.456,"1"]]}
        ]}}"#;
        let series = parse_query_range(body).unwrap();
        assert_eq!(series[0].points[0].0, 1000);
    }

    /// The adapter rejects a disallowed metric BEFORE any network call (defense in
    /// depth at the source, not only at the API edge).
    #[tokio::test]
    async fn prometheus_source_rejects_disallowed_metric_before_network() {
        let src = PrometheusSource::new(
            "http://127.0.0.1:1", // would refuse if dialed
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let err = src.query_range("rate(up[5m])", 0, 10, 1).await.unwrap_err();
        assert!(matches!(err, HistoryError::DisallowedMetric(_)), "{err}");
    }

    /// END-TO-END: the adapter queries a stub HTTP server that returns a matrix and
    /// maps it to TimeSeries. Proves the URL build + httpclient + parse path.
    #[tokio::test]
    async fn prometheus_source_queries_a_stub_and_parses_matrix() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 2048];
            let n = sock.read(&mut sink).await.unwrap();
            let req = String::from_utf8_lossy(&sink[..n]);
            // The request line must carry the constructed, encoded query_range URL.
            assert!(
                req.contains("GET /api/v1/query_range?query=ironcache_used_memory_bytes&start=1000&end=2000&step=15 HTTP/1.1"),
                "request line was: {req}"
            );
            let body = r#"{"status":"success","data":{"resultType":"matrix","result":[
                {"metric":{"__name__":"ironcache_used_memory_bytes"},"values":[[1000,"42"],[1015,"43"]]}
            ]}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let src = PrometheusSource::new(
            &format!("http://{addr}"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        );
        let series = src
            .query_range("ironcache_used_memory_bytes", 1000, 2000, 15)
            .await
            .unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points, vec![(1000, 42.0), (1015, 43.0)]);
        server.abort();
    }

    /// A Prometheus 400 with a JSON error body maps to HistoryError::Source with
    /// the error text (not a transport error).
    #[tokio::test]
    async fn prometheus_source_maps_http_error_to_source_error() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = sock.read(&mut sink).await.unwrap();
            let body = r#"{"status":"error","errorType":"bad_data","error":"invalid step"}"#;
            let resp = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        let src = PrometheusSource::new(
            &format!("http://{addr}"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        );
        let err = src
            .query_range("ironcache_used_memory_bytes", 1, 2, 1)
            .await
            .unwrap_err();
        match err {
            HistoryError::Source(msg) => assert!(msg.contains("invalid step"), "{msg}"),
            other => panic!("expected Source, got {other:?}"),
        }
        server.abort();
    }
}
