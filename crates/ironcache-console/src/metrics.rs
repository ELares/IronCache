// SPDX-License-Identifier: MIT OR Apache-2.0
//! Console SELF-observability (issue #353, review item: "monitor the monitor").
//!
//! The console exposes its OWN `/metrics` so an operator can alert on a stale
//! view: poll success/failure counters and an always-present last-successful-poll
//! unix-time gauge (the central hazard is a console that silently stops
//! refreshing). In PR-1 there is no poller yet, so the counters read zero and the
//! last-poll gauge reads 0 (never) until the first poll lands in #355.
//!
//! All time is taken through the determinism seam (`ironcache-env`), never
//! `Instant::now`/`SystemTime::now` directly, so the invariant lint is satisfied.

use std::sync::atomic::{AtomicU64, Ordering};

use ironcache_env::{Clock, Monotonic, SystemEnv};

/// A boot-anchored clock for uptime and wall-clock stamps, through the Env seam.
struct ClockState {
    env: SystemEnv,
    boot: Monotonic,
}

impl ClockState {
    fn new() -> Self {
        let env = SystemEnv::new();
        let boot = env.now();
        ClockState { env, boot }
    }

    fn uptime_secs(&self) -> u64 {
        self.env
            .now()
            .saturating_duration_since(self.boot)
            .as_secs()
    }

    fn now_unix_millis(&self) -> u64 {
        self.env.now_unix_millis()
    }
}

/// The console's self-metrics. Cheap, lock-free atomics read at scrape time and
/// incremented by the (future) poll loop. Shared by `Arc`.
pub struct ConsoleMetrics {
    clock: ClockState,
    poll_success_total: AtomicU64,
    poll_failure_total: AtomicU64,
    /// Wall-clock millis of the last SUCCESSFUL poll; `0` means "never polled".
    last_poll_unix_millis: AtomicU64,
}

impl Default for ConsoleMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsoleMetrics {
    #[must_use]
    pub fn new() -> Self {
        ConsoleMetrics {
            clock: ClockState::new(),
            poll_success_total: AtomicU64::new(0),
            poll_failure_total: AtomicU64::new(0),
            last_poll_unix_millis: AtomicU64::new(0),
        }
    }

    /// Record one successful node poll and stamp the topology freshness. Used by
    /// the poll loop in #355.
    pub fn record_poll_success(&self) {
        self.poll_success_total.fetch_add(1, Ordering::Relaxed);
        self.last_poll_unix_millis
            .store(self.clock.now_unix_millis(), Ordering::Relaxed);
    }

    /// Record one failed node poll. Used by the poll loop in #355.
    pub fn record_poll_failure(&self) {
        self.poll_failure_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Unix time (seconds) of the last successful poll; `0` means "never polled".
    /// An ALWAYS-present series (the engine's `*_last_save_unixtime` idiom): an
    /// operator alerts on staleness with `time() - metric > N`, and on a console
    /// that never completes a first poll with `metric == 0` (a series that only
    /// appears after the first poll cannot express that "never refreshed" case,
    /// which is exactly the hazard this exists to catch). It is an absolute
    /// wall-clock stamp, so a backward clock step cannot make a stale console
    /// read as fresh (the staleness math lives in PromQL, not here).
    fn last_poll_unix_secs(&self) -> u64 {
        self.last_poll_unix_millis.load(Ordering::Relaxed) / 1000
    }

    /// Render the Prometheus text exposition of the console's self-metrics.
    #[must_use]
    pub fn render(&self) -> String {
        let ver = crate::cli::BUILD_VERSION;
        let uptime = self.clock.uptime_secs();
        let ok = self.poll_success_total.load(Ordering::Relaxed);
        let fail = self.poll_failure_total.load(Ordering::Relaxed);
        let last_poll = self.last_poll_unix_secs();
        format!(
            "# HELP ironcache_console_build_info Build version of the console (always 1).\n\
             # TYPE ironcache_console_build_info gauge\n\
             ironcache_console_build_info{{version=\"{ver}\"}} 1\n\
             # HELP ironcache_console_uptime_seconds Console process uptime in seconds.\n\
             # TYPE ironcache_console_uptime_seconds gauge\n\
             ironcache_console_uptime_seconds {uptime}\n\
             # HELP ironcache_console_poll_success_total Successful node polls since boot.\n\
             # TYPE ironcache_console_poll_success_total counter\n\
             ironcache_console_poll_success_total {ok}\n\
             # HELP ironcache_console_poll_failure_total Failed node polls since boot.\n\
             # TYPE ironcache_console_poll_failure_total counter\n\
             ironcache_console_poll_failure_total {fail}\n\
             # HELP ironcache_console_last_poll_unixtime Unix time of the last successful node poll (0 = never).\n\
             # TYPE ironcache_console_last_poll_unixtime gauge\n\
             ironcache_console_last_poll_unixtime {last_poll}\n"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_has_build_info_and_counters() {
        let m = ConsoleMetrics::new();
        let text = m.render();
        assert!(text.contains("# TYPE ironcache_console_build_info gauge"));
        assert!(text.contains(&format!(
            "ironcache_console_build_info{{version=\"{}\"}} 1",
            crate::cli::BUILD_VERSION
        )));
        assert!(text.contains("ironcache_console_poll_success_total 0"));
        assert!(text.contains("ironcache_console_poll_failure_total 0"));
        assert!(text.contains("ironcache_console_uptime_seconds"));
        // No poll yet: the last-poll gauge is always present and reads 0 (never).
        assert!(text.contains("ironcache_console_last_poll_unixtime 0\n"));
    }

    #[test]
    fn poll_success_increments_and_stamps_last_poll() {
        let m = ConsoleMetrics::new();
        m.record_poll_success();
        m.record_poll_success();
        let text = m.render();
        assert!(text.contains("ironcache_console_poll_success_total 2"));
        // After a successful poll the last-poll gauge is a real (non-zero) stamp.
        assert!(text.contains("ironcache_console_last_poll_unixtime "));
        assert!(!text.contains("ironcache_console_last_poll_unixtime 0\n"));
    }

    #[test]
    fn poll_failure_increments() {
        let m = ConsoleMetrics::new();
        m.record_poll_failure();
        assert!(
            m.render()
                .contains("ironcache_console_poll_failure_total 1")
        );
    }
}
