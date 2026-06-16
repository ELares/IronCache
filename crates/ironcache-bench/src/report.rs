// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hand-written JSON serialization for the load-generator results, matching the
//! A1 memmodel style (no serde dependency).
//!
//! Two result shapes are emitted, one per pass:
//!
//! - [`ClosedLoopResult`]: the run parameters plus the peak throughput (`qps`).
//! - [`OpenLoopResult`]: the run parameters plus the latency tail
//!   (`p50/p99/p999/p9999` in MICROSECONDS) and the recorded sample count.
//!
//! Both carry the SEED so a run is reproducible from its JSON alone.
//!
//! ## Latency unit
//!
//! All latency percentiles are reported in MICROSECONDS (us). The open-loop pass
//! records into an `hdrhistogram::Histogram<u64>` in microseconds (see
//! [`crate::open_loop`]); the JSON field names carry no unit suffix but this module
//! and the open-loop doc fix the unit at microseconds.
//!
//! ## Histogram artifact
//!
//! [`write_histogram_percentiles`] dumps a plain-text percentile table (one
//! `percentile value_us count` row per recorded quantile step) rather than the HDR
//! V2 binary/base64 wire format. This keeps the `hdrhistogram` dependency on its
//! default-features-off core (no `serialization`/`base64`/`flate2`), which keeps the
//! cargo-deny tree minimal; the percentile dump is human-readable and is the format
//! the A3 run script will diff.

#![forbid(unsafe_code)]

use std::io::{Result, Write};

use hdrhistogram::Histogram;

/// The parameters shared by both passes, echoed into the JSON so a result is
/// self-describing and reproducible.
#[derive(Debug, Clone)]
pub struct RunParams {
    /// `"closed"` or `"open"`.
    pub mode: &'static str,
    /// The workload RNG seed (a fixed `--seed` makes the workload byte-reproducible).
    pub seed: u64,
    /// Keyspace size (distinct keys).
    pub keyspace: u64,
    /// Zipf exponent.
    pub theta: f64,
    /// Read fraction (GET share of the op-mix).
    pub read_ratio: f64,
    /// SET value size in bytes.
    pub value_size: usize,
    /// Run duration in seconds.
    pub duration_secs: f64,
}

impl RunParams {
    /// The shared parameter fields as JSON object members (no surrounding braces),
    /// so each result type can splice them into its own object.
    fn members(&self) -> String {
        format!(
            concat!(
                "\"mode\":\"{}\",\"seed\":{},\"keyspace\":{},\"theta\":{:.4},",
                "\"read_ratio\":{:.4},\"value_size\":{},\"duration_secs\":{:.3}"
            ),
            self.mode,
            self.seed,
            self.keyspace,
            self.theta,
            self.read_ratio,
            self.value_size,
            self.duration_secs,
        )
    }
}

/// The closed-loop pass result: peak throughput.
#[derive(Debug, Clone)]
pub struct ClosedLoopResult {
    /// The echoed run parameters (`mode == "closed"`).
    pub params: RunParams,
    /// The number of concurrent connections.
    pub connections: usize,
    /// Total completed ops across all connections.
    pub total_ops: u64,
    /// Wall seconds actually elapsed (measured via `ironcache_env`, not the nominal
    /// duration).
    pub elapsed_secs: f64,
    /// Peak throughput: `total_ops / elapsed_secs`.
    pub qps: f64,
}

impl ClosedLoopResult {
    /// Hand-written JSON (no serde), one flat object.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                "{{{},\"connections\":{},\"total_ops\":{},",
                "\"elapsed_secs\":{:.4},\"qps\":{:.2}}}"
            ),
            self.params.members(),
            self.connections,
            self.total_ops,
            self.elapsed_secs,
            self.qps,
        )
    }
}

/// The open-loop pass result: the coordinated-omission-free latency tail.
#[derive(Debug, Clone)]
pub struct OpenLoopResult {
    /// The echoed run parameters (`mode == "open"`).
    pub params: RunParams,
    /// The target request rate in ops/sec.
    pub target_rate: f64,
    /// The ACHIEVED request rate: recorded samples over the actual elapsed wall time.
    /// When this is materially below `target_rate` the run was generator-limited and
    /// the latency tail reflects the generator backing up, not the server.
    pub achieved_rate: f64,
    /// The actual wall seconds the run took (measured via `ironcache_env`, not the
    /// nominal duration).
    pub elapsed_secs: f64,
    /// `true` when the run was generator-limited (achieved rate materially below
    /// target). When set, the latency tail must NOT be read as a server measurement.
    pub saturated: bool,
    /// The number of recorded latency samples.
    pub count: u64,
    /// 50th percentile latency in microseconds.
    pub p50_us: u64,
    /// 99th percentile latency in microseconds.
    pub p99_us: u64,
    /// 99.9th percentile latency in microseconds.
    pub p999_us: u64,
    /// 99.99th percentile latency in microseconds.
    pub p9999_us: u64,
    /// Maximum recorded latency in microseconds.
    pub max_us: u64,
}

impl OpenLoopResult {
    /// Hand-written JSON (no serde), one flat object. Latency percentiles are in
    /// microseconds; the field names carry the `_us` suffix to make that explicit.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                "{{{},\"target_rate\":{:.2},\"achieved_rate\":{:.2},",
                "\"elapsed_secs\":{:.4},\"saturated\":{},\"count\":{},",
                "\"p50_us\":{},\"p99_us\":{},\"p999_us\":{},\"p9999_us\":{},\"max_us\":{}}}"
            ),
            self.params.members(),
            self.target_rate,
            self.achieved_rate,
            self.elapsed_secs,
            self.saturated,
            self.count,
            self.p50_us,
            self.p99_us,
            self.p999_us,
            self.p9999_us,
            self.max_us,
        )
    }
}

/// Write a plain-text percentile dump of `hist` to `w`. The format is a header
/// line, then one `percentile<TAB>value_us<TAB>cumulative_count` row per HDR
/// iterator step (the library's `iter_quantiles` with 5 ticks per half-distance,
/// which densifies toward the tail). Documented as the artifact format the run
/// script consumes.
pub fn write_histogram_percentiles<W: Write>(w: &mut W, hist: &Histogram<u64>) -> Result<()> {
    writeln!(
        w,
        "# HdrHistogram percentile dump (latency in microseconds)"
    )?;
    writeln!(w, "# total_count={}", hist.len())?;
    writeln!(w, "percentile\tvalue_us\tcumulative_count")?;
    // The real running total: accumulate the per-step delta (`count_since_last_iteration`)
    // so the column is monotonically non-decreasing and the final row equals `hist.len()`.
    // (The previous `count_since_last_iteration() + count_at_value()` double-counted: it
    // added the step delta to the bucket count at the iterated value, which is neither a
    // per-step delta nor a running total.)
    let mut cum: u64 = 0;
    for v in hist.iter_quantiles(5) {
        cum += v.count_since_last_iteration();
        writeln!(
            w,
            "{:.6}\t{}\t{}",
            v.quantile_iterated_to() * 100.0,
            v.value_iterated_to(),
            cum,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_params(mode: &'static str) -> RunParams {
        RunParams {
            mode,
            seed: 1234,
            keyspace: 1_000_000,
            theta: 0.99,
            read_ratio: 0.9,
            value_size: 128,
            duration_secs: 1.5,
        }
    }

    #[test]
    fn closed_json_is_well_formed_and_has_fields() {
        let r = ClosedLoopResult {
            params: sample_params("closed"),
            connections: 50,
            total_ops: 1_000_000,
            elapsed_secs: 1.0,
            qps: 1_000_000.0,
        };
        let j = r.to_json();
        assert!(j.starts_with('{') && j.ends_with('}'));
        assert!(j.contains("\"mode\":\"closed\""));
        assert!(j.contains("\"seed\":1234"));
        assert!(j.contains("\"connections\":50"));
        assert!(j.contains("\"total_ops\":1000000"));
        assert!(j.contains("\"qps\":1000000.00"));
        assert!(j.contains("\"keyspace\":1000000"));
        assert!(j.contains("\"theta\":0.9900"));
    }

    #[test]
    fn open_json_is_well_formed_and_has_fields() {
        let r = OpenLoopResult {
            params: sample_params("open"),
            target_rate: 50_000.0,
            achieved_rate: 49_950.0,
            elapsed_secs: 1.5015,
            saturated: false,
            count: 75_000,
            p50_us: 120,
            p99_us: 900,
            p999_us: 4_000,
            p9999_us: 9_000,
            max_us: 12_345,
        };
        let j = r.to_json();
        assert!(j.starts_with('{') && j.ends_with('}'));
        assert!(j.contains("\"mode\":\"open\""));
        assert!(j.contains("\"target_rate\":50000.00"));
        assert!(j.contains("\"achieved_rate\":49950.00"));
        assert!(j.contains("\"elapsed_secs\":1.5015"));
        assert!(j.contains("\"saturated\":false"));
        assert!(j.contains("\"count\":75000"));
        assert!(j.contains("\"p50_us\":120"));
        assert!(j.contains("\"p99_us\":900"));
        assert!(j.contains("\"p999_us\":4000"));
        assert!(j.contains("\"p9999_us\":9000"));
        assert!(j.contains("\"max_us\":12345"));
    }

    #[test]
    fn histogram_dump_has_header_and_rows() {
        let mut hist: Histogram<u64> = Histogram::new(3).unwrap();
        for v in 1..=1000u64 {
            hist.record(v).unwrap();
        }
        let mut out = Vec::new();
        write_histogram_percentiles(&mut out, &hist).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("# HdrHistogram percentile dump"));
        assert!(text.contains("# total_count=1000"));
        assert!(text.contains("percentile\tvalue_us\tcumulative_count"));
        // The dump reaches the 100th percentile row.
        assert!(text.lines().count() > 5);
    }

    #[test]
    fn histogram_cumulative_column_is_monotonic_and_totals() {
        // The cumulative_count column must be a real running total: monotonically
        // non-decreasing, with the final row equal to hist.len(). This guards the
        // double-counting bug (count_since_last_iteration + count_at_value).
        let mut hist: Histogram<u64> = Histogram::new(3).unwrap();
        // A spread of values so several quantile steps carry real counts.
        for v in 1..=5000u64 {
            hist.record(v).unwrap();
        }
        let mut out = Vec::new();
        write_histogram_percentiles(&mut out, &hist).unwrap();
        let text = String::from_utf8(out).unwrap();

        // Parse the cumulative_count (3rd tab-separated column) from each data row.
        let cums: Vec<u64> = text
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with("percentile"))
            .filter_map(|l| l.split('\t').nth(2))
            .map(|s| s.parse::<u64>().expect("cumulative_count is an integer"))
            .collect();

        assert!(!cums.is_empty(), "should have parsed some data rows");
        // (a) Monotonically non-decreasing.
        for w in cums.windows(2) {
            assert!(
                w[1] >= w[0],
                "cumulative_count must be non-decreasing: {} then {}",
                w[0],
                w[1]
            );
        }
        // (b) The final cumulative equals the total recorded count.
        assert_eq!(
            *cums.last().unwrap(),
            hist.len(),
            "final cumulative_count must equal hist.len()"
        );
    }
}
