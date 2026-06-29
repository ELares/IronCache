// SPDX-License-Identifier: MIT OR Apache-2.0
//! HOTKEYS: the faithful Redis 8.6 hot-key tracking container (#428).
//!
//! `HOTKEYS START METRICS count [CPU] [NET] [COUNT k] [DURATION s] [SAMPLE ratio] [SLOTS ...]`
//! begins a tracking session that attributes per-command CPU time (microseconds) and network bytes
//! to the command's KEYS, ranking the top-K by each metric; `HOTKEYS GET` returns the session
//! totals + the top-K lists; `HOTKEYS STOP` halts accumulation but keeps the data; `HOTKEYS RESET`
//! frees it. The top-K is held in a bounded probabilistic sketch (Space-Saving, Metwally/Agrawal/
//! El Abbadi, ICDT 2005): O(1) amortized weighted update, O(`cap`) memory, no false negatives for a
//! true heavy hitter (a new key inherits the current min count, an overestimate bounded by that min).
//!
//! ## Why this holds the per-PR perf-gate
//!
//! Tracking is OPT-IN (ACL `@admin`/`@slow`/`@dangerous`) and OFF by default. The state is a NODE-
//! level structure shared by `Arc` (mirroring [`crate::SlowLog`] / [`crate::ClientRegistry`]); the
//! per-command HOOK in the serve layer reads ONE relaxed atomic ([`Hotkeys::is_active`]) and
//! short-circuits when inactive, so the default deployment (and the perf-gate, which runs with
//! tracking off) pays a single predict-not-taken load and never touches the lock or the sketch. Only
//! while a session is ACTIVE does a command take the lock; the lock lives behind a justified
//! `// lint-allow` exactly like [`crate::ClientRegistry`] (node-level diagnostic state, off the hot
//! path when inactive). `SAMPLE ratio` (a first-class Redis option) further bounds the active cost:
//! only every `ratio`-th command feeds the sketch (systematic sampling, an unbiased rate estimator).
//!
//! ## Cross-shard scope
//!
//! The state is node-level, so EVERY shard's commands feed the ONE sketch under the lock: the GET
//! reply is a true node-wide top-K (not a per-shard ~1/N slice). The brief lock contention while a
//! multi-shard node is ACTIVELY tracking is acceptable for a temporary diagnostic verb (the default
//! single-shard bar has no contention, and tracking is off in steady state).

use bytes::Bytes;
use core::sync::atomic::{AtomicU8, Ordering};
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard}; // lint-allow: shared-nothing -- node-level diagnostic state, off the hot path (gated by an atomic when inactive), mirroring ClientRegistry.

/// Default top-K when `HOTKEYS START` omits `COUNT` (Redis does not pin a default; 16 is a useful
/// operator default that bounds the reply).
pub const DEFAULT_HOTKEYS_COUNT: usize = 16;

/// The sketch capacity as a multiple of the requested top-K: more counters than K bound the
/// Space-Saving error (the classic m = O(K/eps) sizing). Clamped to [`MIN_SKETCH_CAP`] /
/// [`MAX_SKETCH_CAP`] so memory stays bounded regardless of the requested `COUNT`.
const SKETCH_CAP_FACTOR: usize = 8;
/// The floor on sketch capacity (so a tiny `COUNT` still tracks enough candidates to be accurate).
const MIN_SKETCH_CAP: usize = 64;
/// The ceiling on sketch capacity (so a huge `COUNT` cannot blow node memory): two sketches at this
/// cap hold at most `2 * MAX_SKETCH_CAP` (key, count) pairs.
const MAX_SKETCH_CAP: usize = 8192;

/// The tracking lifecycle state ([`Hotkeys::state`]). `Idle` (never started, or after `RESET`)
/// makes `HOTKEYS GET` return null; `Active` accumulates; `Stopped` preserves data for `GET` but no
/// longer accumulates. Stored as an `AtomicU8` so the per-command hook reads it lock-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum State {
    Idle = 0,
    Active = 1,
    Stopped = 2,
}

/// The `HOTKEYS START` options (#428), parsed by the command handler and handed to [`Hotkeys::start`].
#[derive(Debug, Clone)]
pub struct HotkeysConfig {
    /// Track the CPU-time metric (`by-cpu-time-us`).
    pub cpu: bool,
    /// Track the network-bytes metric (`by-net-bytes`).
    pub net: bool,
    /// Top-K to report per metric.
    pub count: usize,
    /// Sampling ratio: only every `sample_ratio`-th command feeds the sketch (`1` = every command).
    pub sample_ratio: u64,
    /// Auto-stop after this many milliseconds (`0` = until a manual `HOTKEYS STOP`).
    pub duration_ms: u64,
}

/// A bounded weighted top-K sketch (Space-Saving, Metwally et al. 2005). Holds at most `cap`
/// (key -> accumulated weight) counters; a new key seen at capacity EVICTS the current minimum and
/// inherits its count plus the new weight, so a true heavy hitter is never dropped (its reported
/// weight is an overestimate bounded by the evicted minimum).
#[derive(Debug)]
struct SpaceSaving {
    cap: usize,
    counts: HashMap<Bytes, u64>,
}

impl SpaceSaving {
    fn new(count: usize) -> SpaceSaving {
        let cap = count
            .saturating_mul(SKETCH_CAP_FACTOR)
            .clamp(MIN_SKETCH_CAP, MAX_SKETCH_CAP);
        SpaceSaving {
            cap,
            counts: HashMap::new(),
        }
    }

    /// Add `weight` to `key`'s counter (O(1) amortized; O(`cap`) on an eviction, which happens only
    /// when a NEW key arrives at capacity).
    fn add(&mut self, key: &[u8], weight: u64) {
        if let Some(c) = self.counts.get_mut(key) {
            *c = c.saturating_add(weight);
            return;
        }
        if self.counts.len() < self.cap {
            self.counts.insert(Bytes::copy_from_slice(key), weight);
            return;
        }
        // At capacity: evict the current minimum and let the new key inherit its count + weight.
        if let Some((min_key, min_val)) = self
            .counts
            .iter()
            .min_by_key(|&(_, &v)| v)
            .map(|(k, &v)| (k.clone(), v))
        {
            self.counts.remove(&min_key);
            self.counts
                .insert(Bytes::copy_from_slice(key), min_val.saturating_add(weight));
        }
    }

    /// The top-`k` (key, weight) pairs, highest weight first (ties broken by key bytes for a stable,
    /// test-friendly order).
    fn top(&self, k: usize) -> Vec<(Bytes, u64)> {
        let mut v: Vec<(Bytes, u64)> = self
            .counts
            .iter()
            .map(|(key, c)| (key.clone(), *c))
            .collect();
        v.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(k);
        v
    }
}

/// The mutable inner state behind [`Hotkeys::inner`] (config + running totals + the per-metric
/// sketches). Touched only while a session is `Active` (under the lock) or by the rare GET/STOP/RESET
/// admin verbs.
#[derive(Debug)]
struct Inner {
    cfg: HotkeysConfig,
    /// Unix-ms the session started (from the Env clock, passed in by the caller).
    start_unix_ms: u64,
    /// Unix-ms the session stopped (auto or manual); `0` while active.
    stop_unix_ms: u64,
    /// Total command-execution microseconds over ALL commands this session (every command, not just
    /// sampled): the `all-commands-all-slots-us` figure.
    all_us: u64,
    /// Total network bytes over ALL commands this session: `net-bytes-all-commands-all-slots`.
    all_net_bytes: u64,
    /// Commands seen this session (for systematic sampling).
    seen: u64,
    /// Commands actually sampled into the sketches.
    sampled: u64,
    /// The CPU-time top-K sketch (present iff `cfg.cpu`).
    cpu: Option<SpaceSaving>,
    /// The network-bytes top-K sketch (present iff `cfg.net`).
    net: Option<SpaceSaving>,
}

/// The node-level HOTKEYS tracking container (#428). One per node, shared by `Arc` onto every shard's
/// [`crate`] context. See the module docs for the perf-gate rationale.
#[derive(Debug)]
pub struct Hotkeys {
    /// The lifecycle state, read lock-free by the per-command hook ([`Self::is_active`]).
    state: AtomicU8,
    /// The session data, `None` until the first `START`. Behind a justified lock: only locked while
    /// `Active` or by the cold admin verbs.
    inner: Mutex<Option<Inner>>, // lint-allow: shared-nothing -- node-level diagnostic state, off the hot path when inactive (mirrors ClientRegistry).
}

impl Default for Hotkeys {
    fn default() -> Hotkeys {
        Hotkeys::new()
    }
}

/// A consistent point-in-time view of a HOTKEYS session for `HOTKEYS GET` to render. `None` from
/// [`Hotkeys::snapshot`] means no session exists (Idle / after RESET) -> the command replies null.
#[derive(Debug, Clone)]
pub struct HotkeysSnapshot {
    /// Whether the session is still actively accumulating (`tracking-active`).
    pub active: bool,
    /// The configured sampling ratio (`sample-ratio`).
    pub sample_ratio: u64,
    /// Unix-ms the session started (`collection-start-time-unix-ms`).
    pub start_unix_ms: u64,
    /// Milliseconds elapsed since the session started, frozen at STOP for a stopped session
    /// (`collection-duration-ms`).
    pub duration_ms: u64,
    /// `all-commands-all-slots-us`.
    pub all_us: u64,
    /// `net-bytes-all-commands-all-slots`.
    pub all_net_bytes: u64,
    /// Top-K by CPU microseconds (`by-cpu-time-us`), present iff the CPU metric was selected.
    pub cpu: Option<Vec<(Bytes, u64)>>,
    /// Top-K by network bytes (`by-net-bytes`), present iff the NET metric was selected.
    pub net: Option<Vec<(Bytes, u64)>>,
}

impl Hotkeys {
    /// A fresh, idle tracker.
    #[must_use]
    pub fn new() -> Hotkeys {
        Hotkeys {
            state: AtomicU8::new(State::Idle as u8),
            inner: Mutex::new(None),
        }
    }

    /// Whether a session is ACTIVELY accumulating. The per-command hook calls this FIRST: one relaxed
    /// atomic load, so the inactive default path never touches the lock (perf-gate safe). `#[inline]`
    /// so the gate compiles to a single load at the call site.
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::Relaxed) == State::Active as u8
    }

    fn lock(&self) -> MutexGuard<'_, Option<Inner>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Begin a session with `cfg` (started at `now_unix_ms`). Errors if a session is already active
    /// (Redis: "tracking session already in progress"); a START from Idle or Stopped overwrites any
    /// preserved data with a fresh session.
    ///
    /// # Errors
    /// Returns `Err` with a Redis-shaped message when a session is already active.
    pub fn start(&self, cfg: HotkeysConfig, now_unix_ms: u64) -> Result<(), &'static str> {
        if self.is_active() {
            return Err("hotkeys tracking is already in progress");
        }
        let cpu = cfg.cpu.then(|| SpaceSaving::new(cfg.count));
        let net = cfg.net.then(|| SpaceSaving::new(cfg.count));
        *self.lock() = Some(Inner {
            cfg,
            start_unix_ms: now_unix_ms,
            stop_unix_ms: 0,
            all_us: 0,
            all_net_bytes: 0,
            seen: 0,
            sampled: 0,
            cpu,
            net,
        });
        self.state.store(State::Active as u8, Ordering::Relaxed);
        Ok(())
    }

    /// Stop accumulating but PRESERVE the data (`HOTKEYS STOP`). Errors if no session is active.
    ///
    /// # Errors
    /// Returns `Err` when no session is currently active.
    pub fn stop(&self, now_unix_ms: u64) -> Result<(), &'static str> {
        if !self.is_active() {
            return Err("no hotkeys tracking session is active");
        }
        if let Some(inner) = self.lock().as_mut() {
            inner.stop_unix_ms = now_unix_ms;
        }
        self.state.store(State::Stopped as u8, Ordering::Relaxed);
        Ok(())
    }

    /// Release the session's resources (`HOTKEYS RESET`). Errors if a session is still active (Redis:
    /// RESET only when stopped).
    ///
    /// # Errors
    /// Returns `Err` when a session is currently active (must `STOP` first).
    pub fn reset(&self) -> Result<(), &'static str> {
        if self.is_active() {
            return Err("can't reset hotkeys while a tracking session is active");
        }
        *self.lock() = None;
        self.state.store(State::Idle as u8, Ordering::Relaxed);
        Ok(())
    }

    /// Record one command's resource use against its `keys` (the per-command hook, called ONLY after
    /// [`Self::is_active`] returned true). `cpu_us` is the command's execution microseconds and
    /// `net_bytes` its request+reply bytes; both feed the session TOTALS unconditionally, and the
    /// command's keys feed the top-K sketches when the systematic sampler admits this command.
    /// `now_unix_ms` drives DURATION auto-stop.
    pub fn record(&self, keys: &[&[u8]], cpu_us: u64, net_bytes: u64, now_unix_ms: u64) {
        let mut guard = self.lock();
        let Some(inner) = guard.as_mut() else {
            return;
        };
        // Re-check under the lock (STOP/RESET may have raced the is_active() gate).
        if self.state.load(Ordering::Relaxed) != State::Active as u8 {
            return;
        }
        // DURATION auto-stop: once the window elapses, freeze the session as stopped and drop this
        // sample (it belongs after the window).
        if inner.cfg.duration_ms > 0 && now_unix_ms >= inner.start_unix_ms + inner.cfg.duration_ms {
            inner.stop_unix_ms = inner.start_unix_ms + inner.cfg.duration_ms;
            self.state.store(State::Stopped as u8, Ordering::Relaxed);
            return;
        }
        inner.all_us = inner.all_us.saturating_add(cpu_us);
        inner.all_net_bytes = inner.all_net_bytes.saturating_add(net_bytes);
        inner.seen = inner.seen.saturating_add(1);
        // Systematic sampling: admit every `sample_ratio`-th command (ratio <= 1 admits all).
        if inner.cfg.sample_ratio > 1 && inner.seen % inner.cfg.sample_ratio != 0 {
            return;
        }
        inner.sampled = inner.sampled.saturating_add(1);
        if keys.is_empty() {
            return;
        }
        // Attribute the command's weight to EACH of its keys (Redis attributes per touched key).
        for k in keys {
            if let Some(ss) = inner.cpu.as_mut() {
                ss.add(k, cpu_us);
            }
            if let Some(ss) = inner.net.as_mut() {
                ss.add(k, net_bytes);
            }
        }
    }

    /// A snapshot for `HOTKEYS GET` at `now_unix_ms`, or `None` if no session exists (Idle / reset).
    /// Also applies DURATION auto-stop (a GET after the window reports the session as inactive).
    #[must_use]
    pub fn snapshot(&self, now_unix_ms: u64) -> Option<HotkeysSnapshot> {
        let mut guard = self.lock();
        let inner = guard.as_mut()?;
        let mut active = self.state.load(Ordering::Relaxed) == State::Active as u8;
        if active
            && inner.cfg.duration_ms > 0
            && now_unix_ms >= inner.start_unix_ms + inner.cfg.duration_ms
        {
            inner.stop_unix_ms = inner.start_unix_ms + inner.cfg.duration_ms;
            self.state.store(State::Stopped as u8, Ordering::Relaxed);
            active = false;
        }
        let effective_now = if active {
            now_unix_ms
        } else {
            inner.stop_unix_ms.max(inner.start_unix_ms)
        };
        Some(HotkeysSnapshot {
            active,
            sample_ratio: inner.cfg.sample_ratio,
            start_unix_ms: inner.start_unix_ms,
            duration_ms: effective_now.saturating_sub(inner.start_unix_ms),
            all_us: inner.all_us,
            all_net_bytes: inner.all_net_bytes,
            cpu: inner.cpu.as_ref().map(|ss| ss.top(inner.cfg.count)),
            net: inner.net.as_ref().map(|ss| ss.top(inner.cfg.count)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(cpu: bool, net: bool, count: usize, ratio: u64, dur: u64) -> HotkeysConfig {
        HotkeysConfig {
            cpu,
            net,
            count,
            sample_ratio: ratio,
            duration_ms: dur,
        }
    }

    #[test]
    fn inactive_is_a_single_atomic_and_snapshot_is_none() {
        let h = Hotkeys::new();
        assert!(!h.is_active());
        assert!(h.snapshot(1000).is_none(), "idle -> null");
        // record on an idle tracker is a no-op (the real hook never calls it when inactive).
        h.record(&[b"k"], 5, 10, 1000);
        assert!(h.snapshot(1000).is_none());
    }

    #[test]
    fn start_record_get_ranks_top_k_by_each_metric() {
        let h = Hotkeys::new();
        h.start(cfg(true, true, 2, 1, 0), 1_000).unwrap();
        assert!(h.is_active());
        // hot: big cpu, small net. heavy: small cpu, big net. cold: tiny both.
        for _ in 0..3 {
            h.record(&[b"hot"], 100, 1, 1_010);
        }
        for _ in 0..3 {
            h.record(&[b"heavy"], 1, 100, 1_010);
        }
        h.record(&[b"cold"], 1, 1, 1_010);
        let snap = h.snapshot(1_050).unwrap();
        assert!(snap.active);
        assert_eq!(snap.sample_ratio, 1);
        assert_eq!(snap.start_unix_ms, 1_000);
        assert_eq!(snap.duration_ms, 50);
        // Totals count EVERY command: 3*100 + 3*1 + 1 = 304 us; net 3*1 + 3*100 + 1 = 304.
        assert_eq!(snap.all_us, 304);
        assert_eq!(snap.all_net_bytes, 304);
        // by-cpu-time-us top-2: hot (300) then heavy/cold; hot is #1.
        let cpu = snap.cpu.unwrap();
        assert_eq!(cpu[0], (Bytes::from_static(b"hot"), 300));
        // by-net-bytes top-2: heavy (300) is #1.
        let net = snap.net.unwrap();
        assert_eq!(net[0], (Bytes::from_static(b"heavy"), 300));
    }

    #[test]
    fn only_selected_metrics_are_present() {
        let h = Hotkeys::new();
        h.start(cfg(true, false, 4, 1, 0), 0).unwrap();
        h.record(&[b"k"], 7, 9, 0);
        let snap = h.snapshot(0).unwrap();
        assert!(snap.cpu.is_some(), "cpu selected");
        assert!(snap.net.is_none(), "net not selected");
        // The net TOTAL is still counted even when the net sketch is absent.
        assert_eq!(snap.all_net_bytes, 9);
    }

    #[test]
    fn systematic_sampling_admits_every_ratio_th_command() {
        let h = Hotkeys::new();
        h.start(cfg(true, false, 8, 3, 0), 0).unwrap(); // sample 1 in 3
        for _ in 0..9 {
            h.record(&[b"k"], 10, 0, 0);
        }
        let snap = h.snapshot(0).unwrap();
        // Totals see all 9 (90 us); the sketch saw every 3rd command -> 3 samples -> 30 us for k.
        assert_eq!(snap.all_us, 90);
        assert_eq!(snap.cpu.unwrap()[0], (Bytes::from_static(b"k"), 30));
    }

    #[test]
    fn duration_auto_stops_and_freezes() {
        let h = Hotkeys::new();
        h.start(cfg(true, false, 4, 1, 100), 1_000).unwrap(); // 100ms window
        h.record(&[b"k"], 5, 0, 1_050); // inside window
        // A record past the window auto-stops and is dropped.
        h.record(&[b"k"], 999, 0, 1_200);
        assert!(!h.is_active(), "auto-stopped after duration");
        let snap = h.snapshot(5_000).unwrap();
        assert!(!snap.active);
        assert_eq!(snap.duration_ms, 100, "frozen at the window length");
        assert_eq!(snap.all_us, 5, "the post-window sample was dropped");
    }

    #[test]
    fn lifecycle_transitions_and_errors() {
        let h = Hotkeys::new();
        // STOP/RESET before START.
        assert!(h.stop(0).is_err(), "stop with no session errors");
        h.start(cfg(true, false, 4, 1, 0), 0).unwrap();
        // Double START errors.
        assert!(h.start(cfg(true, false, 4, 1, 0), 0).is_err());
        // RESET while active errors.
        assert!(h.reset().is_err());
        // STOP preserves data (GET still returns it).
        h.record(&[b"k"], 3, 0, 0);
        h.stop(10).unwrap();
        assert!(!h.is_active());
        let snap = h.snapshot(99).unwrap();
        assert!(!snap.active);
        assert_eq!(snap.all_us, 3, "stop preserves data");
        // RESET when stopped -> idle -> GET null.
        h.reset().unwrap();
        assert!(h.snapshot(99).is_none());
    }

    #[test]
    fn space_saving_keeps_heavy_hitter_under_capacity_pressure() {
        // cap = max(1*8, 64) = 64. Push 200 distinct cold keys + one hammered hot key; the hot key
        // must survive (Space-Saving never drops a true heavy hitter).
        let h = Hotkeys::new();
        h.start(cfg(true, false, 1, 1, 0), 0).unwrap();
        for _ in 0..500 {
            h.record(&[b"HOT"], 50, 0, 0);
        }
        for i in 0..200u32 {
            let k = format!("cold{i}");
            h.record(&[k.as_bytes()], 1, 0, 0);
        }
        let top = h.snapshot(0).unwrap().cpu.unwrap();
        assert_eq!(
            top[0].0,
            Bytes::from_static(b"HOT"),
            "heavy hitter retained"
        );
        assert!(top[0].1 >= 25_000, "hot weight preserved, got {}", top[0].1);
    }
}
