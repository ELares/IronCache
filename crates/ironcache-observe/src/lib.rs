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

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use ironcache_env::Clock;
use std::sync::Arc;
use std::sync::Mutex; // lint-allow: shared-nothing -- the ops/sec sampler ring is node-level cold state, taken only on the rare INFO/metrics READ, never on the per-command hot path (see `OpsPerSecSampler`).

/// Operator-introspection state (PROD-7): the SLOWLOG ring, the LATENCY monitor, and the
/// live-connection registry CLIENT KILL/PAUSE act through. Kept in its own module since each is a
/// small node-level structure behind one justified lock (off the per-command hot path); see
/// [`ops`] for the shared-nothing carve-out rationale.
pub mod ops;

pub use ops::{
    ClientHandle, ClientRegistry, DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_LEN,
    LATENCY_COMMAND_FLOOR_MICROS, LatencyMonitor, SlowLog, SlowLogEntry,
};

/// HOTKEYS: the faithful Redis 8.6 hot-key tracking container (#428). A node-level structure gated
/// by one atomic when inactive, so the default (tracking-off) hot path and the perf-gate are
/// unaffected; see [`hotkeys`] for the full rationale.
pub mod hotkeys;

pub use hotkeys::{DEFAULT_HOTKEYS_COUNT, Hotkeys, HotkeysConfig, HotkeysSnapshot};

/// The IronCache server version reported in `INFO` and `HELLO`. Sourced from the
/// crate version at build time.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// One command's execution tally for INFO `COMMANDSTATS` (#413): the Redis `cmdstat_<cmd>`
/// fields. `calls`/`usec` accumulate over every execution; `usec_per_call` is derived at render.
#[derive(Debug, Default, Clone, Copy)]
pub struct CmdStat {
    /// Total executions (Redis `calls`).
    pub calls: u64,
    /// Total microseconds spent across those executions (Redis `usec`).
    pub usec: u64,
    /// Executions REJECTED before the command body ran (Redis `rejected_calls`). IronCache does
    /// not yet split pre-execution rejections from in-command failures at this layer, so this is
    /// reported as 0 (a documented approximation; the field shape is exact so parsers are happy).
    pub rejected_calls: u64,
    /// Executions that ran but returned an ERROR reply (Redis `failed_calls`).
    pub failed_calls: u64,
}

/// Per-shard command + error execution stats for INFO `COMMANDSTATS` / `ERRORSTATS` (#413).
/// PER-SHARD and home-shard-local for INFO (the same scope the other INFO counters use): the
/// serve loop records each executed command's elapsed micros + whether its reply was an error,
/// and INFO renders the serving shard's table. Read only on the owning shard (NOT cross-thread
/// like [`ShardCountersCell`]), so a plain map with no atomics. `CONFIG RESETSTAT` clears it.
///
/// The command key is the registry's `&'static` canonical name, so a record allocates nothing;
/// the error key is the error CODE (the first whitespace-delimited token, e.g. `ERR` /
/// `WRONGTYPE` / `NOPERM`), owned because it is parsed from the reply.
#[derive(Debug, Default)]
pub struct CommandStats {
    cmds: std::collections::HashMap<&'static [u8], CmdStat>,
    errors: std::collections::HashMap<Box<[u8]>, u64>,
}

impl CommandStats {
    /// Record one EXECUTED command: `name` is the registry canonical name (a `&'static`, so the
    /// key allocates nothing), `usec` its elapsed micros, `failed` whether its reply was an error.
    pub fn record(&mut self, name: &'static [u8], usec: u64, failed: bool) {
        let e = self.cmds.entry(name).or_default();
        e.calls = e.calls.saturating_add(1);
        e.usec = e.usec.saturating_add(usec);
        if failed {
            e.failed_calls = e.failed_calls.saturating_add(1);
        }
    }

    /// Record one error reply by its CODE (the first token of the error line, uppercase by Redis
    /// convention), for INFO ERRORSTATS.
    pub fn record_error(&mut self, code: &[u8]) {
        *self.errors.entry(code.into()).or_insert(0) += 1;
    }

    /// Clear every command + error tally (`CONFIG RESETSTAT`).
    pub fn reset(&mut self) {
        self.cmds.clear();
        self.errors.clear();
    }

    /// Append the `# Commandstats` section BODY (no header) to `out`: one
    /// `cmdstat_<lowercased name>:calls=N,usec=N,usec_per_call=N.NN,rejected_calls=N,failed_calls=N`
    /// line per command, matching the Redis field shape go-redis / redis-py parse. Sorted by name
    /// so the output is deterministic.
    pub fn render_commandstats(&self, out: &mut String) {
        use core::fmt::Write;
        let mut names: Vec<&&'static [u8]> = self.cmds.keys().collect();
        names.sort_unstable();
        for name in names {
            let s = self.cmds[*name];
            let lname = String::from_utf8_lossy(name).to_ascii_lowercase();
            #[allow(clippy::cast_precision_loss)]
            let per_call = if s.calls == 0 {
                0.0
            } else {
                s.usec as f64 / s.calls as f64
            };
            let _ = write!(
                out,
                "cmdstat_{lname}:calls={},usec={},usec_per_call={per_call:.2},rejected_calls={},failed_calls={}\r\n",
                s.calls, s.usec, s.rejected_calls, s.failed_calls
            );
        }
    }

    /// Append the `# Errorstats` section BODY (no header) to `out`: one `errorstat_<CODE>:count=N`
    /// line per error code (Redis shape). Sorted by code for determinism.
    pub fn render_errorstats(&self, out: &mut String) {
        use core::fmt::Write;
        let mut codes: Vec<&Box<[u8]>> = self.errors.keys().collect();
        codes.sort_unstable();
        for code in codes {
            let c = String::from_utf8_lossy(code);
            let _ = write!(
                out,
                "errorstat_{c}:count={}\r\n",
                self.errors[code.as_ref()]
            );
        }
    }
}

/// The number of latency histogram buckets (#546): the 18 finite log-spaced `le` upper bounds
/// plus the terminal `+Inf` overflow bucket. Fixed at compile time so the per-shard histogram is
/// a pre-sized atomic array (no per-command allocation, the hot-path requirement).
pub const LATENCY_BUCKET_COUNT: usize = 19;

/// The finite bucket UPPER BOUNDS in MICROSECONDS (#546), log-spaced ~25us..10s. The command
/// serve loop measures elapsed micros already (SLOWLOG + COMMANDSTATS), so the find-bucket compares
/// integers directly with no unit conversion on the hot path. The matching Prometheus `le` labels
/// (in SECONDS, the base unit for a `_seconds` histogram) are [`LATENCY_BUCKET_LE`], and there are
/// `LATENCY_BUCKET_COUNT - 1` of these (the `+Inf` bucket has no finite bound).
const LATENCY_BUCKET_BOUNDS_US: [u64; LATENCY_BUCKET_COUNT - 1] = [
    25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// The Prometheus `le` label strings (in SECONDS) for each bucket, parallel to
/// [`LATENCY_BUCKET_BOUNDS_US`] with the terminal `+Inf`. Pre-formatted `&'static str`s so the
/// render allocates nothing per bucket boundary; kept as literals (not derived from the micros
/// bounds) so the exposition text is exact and stable.
pub const LATENCY_BUCKET_LE: [&str; LATENCY_BUCKET_COUNT] = [
    "0.000025", "0.00005", "0.0001", "0.00025", "0.0005", "0.001", "0.0025", "0.005", "0.01",
    "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf",
];

/// One shard's command-latency HISTOGRAM storage (#546): a fixed array of `AtomicU64` bucket
/// counters plus the running micros SUM and observation COUNT. Lives inside [`ShardCountersCell`]
/// so the shared-nothing writer (the serving shard, one writer per cell) records with the SAME
/// uncontended `Relaxed` atomics the counters use, while the out-of-band metrics task reads every
/// shard's histogram across threads for the `/metrics` scrape.
///
/// ## Semantics
///
/// `buckets[i]` holds the NON-cumulative count of observations that fell in bucket `i` (elapsed
/// `<=` [`LATENCY_BUCKET_BOUNDS_US`]`[i]` and above the previous bound; the LAST index is the `+Inf`
/// overflow). The Prometheus `le` series is CUMULATIVE, so the render accumulates these (see
/// [`LatencyHistogram::cumulative`]); storing per-bucket keeps a record to ONE atomic add.
/// `count` is a DEDICATED observation count (not the shared command counter), so the histogram is
/// self-consistent by construction: the sum of the buckets equals `count`, hence the `+Inf` bucket
/// equals `_count`. `sum_micros` totals elapsed micros; the render divides by 1e6 for the `_sum` in
/// seconds. `Relaxed` is correct for the same reason the counters use it (independent monotonic
/// tallies read at a fuzzy scrape instant).
#[derive(Debug)]
struct LatencyHistogramCell {
    /// Per-bucket (NON-cumulative) observation counts; index `LATENCY_BUCKET_COUNT - 1` is `+Inf`.
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    /// Total elapsed MICROSECONDS across every observation (rendered as `_sum` in seconds).
    sum_micros: AtomicU64,
    /// Total number of observations (the histogram `_count`; equals the sum of `buckets`).
    count: AtomicU64,
}

impl Default for LatencyHistogramCell {
    fn default() -> Self {
        LatencyHistogramCell {
            // `AtomicU64` is not `Copy`, so the array cannot be built with a repeat literal; build
            // each cell explicitly. A one-time boot cost (one array per shard), never per command.
            buckets: core::array::from_fn(|_| AtomicU64::new(0)),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogramCell {
    /// Record one command's `elapsed_us` (#546): a branch-predictable find-bucket, one `Relaxed`
    /// bucket increment, one `Relaxed` sum add, and one `Relaxed` count bump. The find-bucket is a
    /// data-independent fold (count the finite bounds strictly BELOW `elapsed_us`), which yields the
    /// index of the smallest bucket whose `le` bound is `>=` the observation -- exactly Prometheus
    /// `le` semantics -- and saturates at the `+Inf` index when the elapsed exceeds every bound. No
    /// allocation, no new clock read (the caller reuses the SLOWLOG/COMMANDSTATS elapsed measurement).
    fn observe(&self, elapsed_us: u64) {
        let idx = LATENCY_BUCKET_BOUNDS_US
            .iter()
            .filter(|&&bound| elapsed_us > bound)
            .count();
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(elapsed_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Read this shard's histogram into an immutable, summable [`LatencyHistogram`] (each load
    /// `Relaxed`, like [`ShardCountersCell::snapshot`]).
    fn snapshot(&self) -> LatencyHistogram {
        let mut buckets = [0u64; LATENCY_BUCKET_COUNT];
        for (dst, src) in buckets.iter_mut().zip(self.buckets.iter()) {
            *dst = src.load(Ordering::Relaxed);
        }
        LatencyHistogram {
            buckets,
            sum_micros: self.sum_micros.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

/// An immutable, summable snapshot of one shard's (or the node-wide) command-latency histogram
/// (#546). Held OUTSIDE [`CounterSnapshot`] (which stays `Copy` and small) because the bucket array
/// is comparatively large and the counter snapshot is merged/copied on many paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyHistogram {
    /// Per-bucket (NON-cumulative) observation counts, parallel to [`LATENCY_BUCKET_LE`]; the last
    /// entry is the `+Inf` overflow. [`LatencyHistogram::cumulative`] turns these into the
    /// cumulative Prometheus `le` series.
    pub buckets: [u64; LATENCY_BUCKET_COUNT],
    /// Total elapsed MICROSECONDS across every observation (the render divides by 1e6 for `_sum`).
    pub sum_micros: u64,
    /// Total observations (the histogram `_count`; equals the sum of `buckets`, hence `+Inf`).
    pub count: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram {
            buckets: [0; LATENCY_BUCKET_COUNT],
            sum_micros: 0,
            count: 0,
        }
    }
}

impl LatencyHistogram {
    /// Fold another histogram into this one element-wise (the cross-shard rollup, mirroring
    /// [`CounterSnapshot::merge`]): buckets add position-wise, and the sum + count add.
    #[must_use]
    pub fn merge(mut self, other: &LatencyHistogram) -> LatencyHistogram {
        for (dst, src) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *dst += *src;
        }
        self.sum_micros += other.sum_micros;
        self.count += other.count;
        self
    }

    /// The CUMULATIVE bucket counts (Prometheus `le` semantics): entry `i` is the number of
    /// observations `<=` bound `i`. Monotonic non-decreasing by construction, and the final entry
    /// (the `+Inf` bucket) equals [`Self::count`].
    #[must_use]
    pub fn cumulative(&self) -> [u64; LATENCY_BUCKET_COUNT] {
        let mut out = [0u64; LATENCY_BUCKET_COUNT];
        let mut acc = 0u64;
        for (dst, &b) in out.iter_mut().zip(self.buckets.iter()) {
            acc += b;
            *dst = acc;
        }
        out
    }

    /// The total observed latency in SECONDS (the `_sum` value), converted from the micros total.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sum_seconds(&self) -> f64 {
        self.sum_micros as f64 / 1_000_000.0
    }
}

/// The capacity of a shard's per-command COMMANDSTATS table (#527): one atomic [`CmdStatCell`]
/// SLOT per client command, indexed by the server's STABLE command ordinal (its position in the
/// server's client-command list). Sized with headroom over the ~200 client commands; the server
/// asserts its command count fits (`command_stat_slots_fit_the_registry`). Slots the server never
/// indexes stay zero, so the node-wide rollup + render simply skip them. This crate does not know
/// the command NAMES (it is below the server in the dep graph), only the opaque ordinal.
pub const COMMAND_STAT_SLOTS: usize = 256;

/// One command's cross-shard-readable execution tally (#527): the atomic backing of a [`CmdStat`],
/// one per command SLOT in a [`ShardCountersCell`]. Written ONLY by the owning shard's serve loop
/// (uncontended `Relaxed` adds -- the SAME single-increment the counters use, no lock and no
/// allocation) and READ across threads by the node-wide INFO COMMANDSTATS rollup
/// ([`MetricsRegistry::aggregate_command_stats`]). `rejected_calls` is not yet split out at this
/// layer (a documented approximation, matching [`CmdStat`]), so only `calls`/`usec`/`failed_calls`
/// are ever bumped; the reset zeroes all four.
#[derive(Debug, Default)]
struct CmdStatCell {
    calls: AtomicU64,
    usec: AtomicU64,
    rejected_calls: AtomicU64,
    failed_calls: AtomicU64,
}

impl CmdStatCell {
    /// Record one executed command into this slot: one `Relaxed` calls-increment + a usec add, plus
    /// a failed-increment when the reply was an error. No lock, no allocation (the slot exists).
    fn record(&self, usec: u64, failed: bool) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.usec.fetch_add(usec, Ordering::Relaxed);
        if failed {
            self.failed_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read this slot into an immutable, summable [`CmdStat`] (each load `Relaxed`).
    fn snapshot(&self) -> CmdStat {
        CmdStat {
            calls: self.calls.load(Ordering::Relaxed),
            usec: self.usec.load(Ordering::Relaxed),
            rejected_calls: self.rejected_calls.load(Ordering::Relaxed),
            failed_calls: self.failed_calls.load(Ordering::Relaxed),
        }
    }

    /// Zero this slot (`CONFIG RESETSTAT`), each store `Relaxed`.
    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.usec.store(0, Ordering::Relaxed);
        self.rejected_calls.store(0, Ordering::Relaxed);
        self.failed_calls.store(0, Ordering::Relaxed);
    }
}

/// The per-shard counter STORAGE: a flat bag of `AtomicU64`s, one cell per counter, owned
/// by ONE shard and shared (by `Arc`) into the process-wide [`MetricsRegistry`] so the
/// out-of-band metrics HTTP task can READ the live values from another thread WITHOUT a lock
/// (it is the lock-free way to expose per-shard state from the shared-nothing model, exactly
/// like the raft control plane's `Copy` status snapshot).
///
/// ## Why atomics, and why this is NOT new hot-path cost
///
/// A shard mutates only ITS OWN cell, so every store is UNCONTENDED: the cache line is owned
/// by that core, and a `Relaxed` `fetch_add` on an uncontended line is the same single
/// increment the prior plain `u64 += 1` compiled to, with no lock and no cross-core traffic.
/// This is the canonical Prometheus-counter pattern (a per-core counter read by a scraper).
/// `Relaxed` is correct: the counters are independent monotonic tallies with no
/// happens-before relationship to publish, and the reader tolerates reading them at slightly
/// different instants (a metrics scrape is inherently a fuzzy snapshot). `connected_clients`
/// is a GAUGE (a `fetch_sub` on close), so it uses `saturating`-style guards via a CAS-free
/// `fetch_sub` that never underflows in practice (open precedes close on the same shard).
#[derive(Debug)]
pub struct ShardCountersCell {
    connections_received: AtomicU64,
    commands_processed: AtomicU64,
    connected_clients: AtomicU64,
    /// Connections PARKED on a blocking command (BLPOP/BRPOP/BLMOVE/WAIT, ...) right now (INFO
    /// `blocked_clients`, #661). A GAUGE like `connected_clients`: incremented when a client parks,
    /// decremented on EVERY park exit (wake / timeout / disconnect) via [`BlockedGuard`]'s Drop, so
    /// no exit path can leak the count. Summed across shards for the node-wide INFO figure.
    blocked_clients: AtomicU64,
    /// bumped once per serve RECV on both datapaths (tokio + io_uring). `redis_exporter` reads
    /// `redis_net_input_bytes_total` from this INFO field. A since-start stat (`CONFIG RESETSTAT`
    /// clears it, matching Redis's `resetServerStats`).
    total_net_input_bytes: AtomicU64,
    /// Cumulative bytes WRITTEN to this shard's client sockets (INFO `total_net_output_bytes`,
    /// #527): bumped once per serve SEND on both datapaths. `redis_exporter` reads
    /// `redis_net_output_bytes_total` from this INFO field. Resettable like the input total.
    total_net_output_bytes: AtomicU64,
    evicted_keys: AtomicU64,
    expired_keys: AtomicU64,
    keyspace_hits: AtomicU64,
    keyspace_misses: AtomicU64,
    /// This shard's live KEY COUNT (sum of its per-DB lengths), a GAUGE published OFF the
    /// command hot path by the shard's periodic active-expiry tick (and on connection close), so
    /// the `/metrics` keyspace gauge is eventually-consistent (bounded by the expiry cycle) at
    /// ZERO per-command cost. The metrics task sums it across shards for the node-wide keyspace.
    keyspace_keys: AtomicU64,
    /// COORDINATOR HOP counters (#556) -- the #517 zero-hop MEASUREMENT harness. Same shared-nothing
    /// `Relaxed`-atomic pattern as the counters above (one writer per counter, uncontended on this
    /// shard's own cache line), so each is the SAME single increment a plain `u64 += 1` compiled to.
    ///
    /// * `hops_sent`: cross-shard requests THIS shard DISPATCHED to a peer's inbox (the hop it PAID),
    ///   bumped on the coordinator's single-target keyed hop branch in `route_and_dispatch`.
    /// * `hops_served`: cross-shard requests THIS shard RECEIVED + served for a peer, bumped by the
    ///   drain loop when it runs a peer's remote keyed / whole-keyspace unit (`run_remote`).
    /// * `local_served`: keyed requests served LOCALLY (owner == home shard, NO hop), the complement
    ///   of `hops_sent` -- so hop-rate = `hops_sent / (hops_sent + local_served)` is derivable.
    ///
    /// In shard-owners mode (#517) a cluster-aware client dialing owner ports drives `hops_sent` to
    /// ~0 (every key homes on its owner), which is now a metric an operator/test can ASSERT instead
    /// of a claim. The inbox-DEPTH gauge is deliberately NOT stored here: it is sampled from the mpsc
    /// channel length at scrape time (see `render_inbox_depth`), so it costs the hop path nothing.
    hops_sent: AtomicU64,
    hops_served: AtomicU64,
    local_served: AtomicU64,
    /// This shard's command-latency HISTOGRAM (#546): a fixed atomic bucket array + micros sum +
    /// observation count, recorded per command by the serve loop (reusing the SLOWLOG/COMMANDSTATS
    /// elapsed micros, no new clock read) and summed across shards by the metrics task for the
    /// `/metrics` histogram series. Not touched by [`Self::reset_stats`]: a Prometheus histogram is a
    /// cumulative, counter-typed family expected to run for the process lifetime (rate/quantile math
    /// assumes it only resets on restart), so `CONFIG RESETSTAT` leaves it intact.
    latency: LatencyHistogramCell,
    /// This shard's per-command COMMANDSTATS table (#527): [`COMMAND_STAT_SLOTS`] atomic
    /// [`CmdStatCell`]s indexed by the server's stable command ordinal, so the node-wide INFO
    /// COMMANDSTATS rollup ([`MetricsRegistry::aggregate_command_stats`]) sums each command's
    /// calls/usec/failed across shards -- the per-command analog of the top-level `# Stats` rollup
    /// (#545). Written on the command hot path by the SERVING shard through `Relaxed` adds (no lock,
    /// no allocation -- the slot is pre-allocated), read cross-thread by the rollup. `CONFIG
    /// RESETSTAT` clears it (a since-start stat, like the counters).
    cmd_stats: Box<[CmdStatCell]>,
}

impl Default for ShardCountersCell {
    fn default() -> Self {
        // Manual (not derived): the `cmd_stats` slice must be SIZED to [`COMMAND_STAT_SLOTS`] (a
        // derived `Box<[_]>::default()` is empty), and `AtomicU64`/`CmdStatCell` are not `Copy` so
        // the slots are built explicitly. A one-time boot cost per shard, never per command.
        ShardCountersCell {
            connections_received: AtomicU64::new(0),
            commands_processed: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            blocked_clients: AtomicU64::new(0),
            total_net_input_bytes: AtomicU64::new(0),
            total_net_output_bytes: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            keyspace_keys: AtomicU64::new(0),
            hops_sent: AtomicU64::new(0),
            hops_served: AtomicU64::new(0),
            local_served: AtomicU64::new(0),
            latency: LatencyHistogramCell::default(),
            cmd_stats: (0..COMMAND_STAT_SLOTS)
                .map(|_| CmdStatCell::default())
                .collect(),
        }
    }
}

impl ShardCountersCell {
    /// Read this shard's cell into an immutable, summable [`CounterSnapshot`]. Used by the
    /// out-of-band metrics task (cross-thread) AND by the same-shard INFO rollup. Each load is
    /// `Relaxed` (see the type docs); reading the cells at slightly different instants is fine
    /// for a metrics scrape.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received.load(Ordering::Relaxed),
            commands_processed: self.commands_processed.load(Ordering::Relaxed),
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
            blocked_clients: self.blocked_clients.load(Ordering::Relaxed),
            total_net_input_bytes: self.total_net_input_bytes.load(Ordering::Relaxed),
            total_net_output_bytes: self.total_net_output_bytes.load(Ordering::Relaxed),
            evicted_keys: self.evicted_keys.load(Ordering::Relaxed),
            expired_keys: self.expired_keys.load(Ordering::Relaxed),
            keyspace_hits: self.keyspace_hits.load(Ordering::Relaxed),
            keyspace_misses: self.keyspace_misses.load(Ordering::Relaxed),
            keyspace_keys: self.keyspace_keys.load(Ordering::Relaxed),
            hops_sent: self.hops_sent.load(Ordering::Relaxed),
            hops_served: self.hops_served.load(Ordering::Relaxed),
            local_served: self.local_served.load(Ordering::Relaxed),
        }
    }

    /// Record one command's elapsed micros into this shard's latency HISTOGRAM (#546): a
    /// branch-predictable find-bucket + one `Relaxed` bucket increment + a `Relaxed` sum add + a
    /// `Relaxed` count bump (see [`LatencyHistogramCell::observe`]). Called on the command hot path
    /// with the SAME elapsed the SLOWLOG/COMMANDSTATS hooks already computed (no new clock read).
    pub fn observe_latency(&self, elapsed_us: u64) {
        self.latency.observe(elapsed_us);
    }

    /// Read this shard's latency histogram into an immutable, summable [`LatencyHistogram`] (the
    /// cross-thread read the metrics task folds across shards).
    #[must_use]
    pub fn latency_snapshot(&self) -> LatencyHistogram {
        self.latency.snapshot()
    }

    /// Read this shard's per-command COMMANDSTATS table into a summable per-slot vector (#527), the
    /// cross-thread read the node-wide rollup folds. `out[ordinal]` is this shard's tally for the
    /// command at that ordinal; each load is `Relaxed` (see the type docs).
    #[must_use]
    pub fn command_stats_snapshot(&self) -> Vec<CmdStat> {
        self.cmd_stats.iter().map(CmdStatCell::snapshot).collect()
    }

    /// Publish this shard's live key count (a GAUGE store, `Relaxed`). Called off the command
    /// hot path (the periodic expiry tick + connection close), so it adds no per-command cost.
    pub fn set_keyspace_keys(&self, keys: u64) {
        self.keyspace_keys.store(keys, Ordering::Relaxed);
    }

    /// Zero the RESETTABLE stat counters (`CONFIG RESETSTAT`, #531): the eviction / expiry /
    /// keyspace hit-miss totals and the command / connection COUNTERS, the SAME set Redis
    /// `resetServerStats` clears. It DELIBERATELY leaves `connected_clients` (a live gauge, not a
    /// since-reset stat) and `keyspace_keys` (a live key-count gauge) untouched, matching
    /// [`ShardCounters::apply`]'s per-shard reset. The registry fans this across EVERY cell so a
    /// NODE-WIDE INFO rollup ([`MetricsRegistry::aggregate`]) actually zeroes -- see
    /// [`MetricsRegistry::reset_stats`]; each store is `Relaxed` (independent monotonic tallies,
    /// the same relaxation the increments use).
    pub fn reset_stats(&self) {
        self.evicted_keys.store(0, Ordering::Relaxed);
        self.expired_keys.store(0, Ordering::Relaxed);
        self.keyspace_hits.store(0, Ordering::Relaxed);
        self.keyspace_misses.store(0, Ordering::Relaxed);
        self.commands_processed.store(0, Ordering::Relaxed);
        self.connections_received.store(0, Ordering::Relaxed);
        // The net-io byte totals (#527) are since-start `total_*` stats like `commands_processed`,
        // so RESETSTAT clears them too (Redis `resetServerStats` zeroes `stat_net_input_bytes` /
        // `stat_net_output_bytes`).
        self.total_net_input_bytes.store(0, Ordering::Relaxed);
        self.total_net_output_bytes.store(0, Ordering::Relaxed);
        // The per-command COMMANDSTATS table (#527) is the `cmdstat_*` since-start tally Redis
        // `resetServerStats` also clears; the registry fans this across every shard's cell so the
        // node-wide COMMANDSTATS rollup zeroes (#531 parity with the `# Stats` reset).
        for slot in &self.cmd_stats {
            slot.reset();
        }
        // The coordinator HOP counters (#556) are `_total` since-start stats like
        // `commands_processed`, so `CONFIG RESETSTAT` clears them too (an operator resetting stats
        // expects the hop-rate window to restart with them). The inbox-depth GAUGE is not a stat
        // and is sampled live, so there is nothing to reset for it.
        self.hops_sent.store(0, Ordering::Relaxed);
        self.hops_served.store(0, Ordering::Relaxed);
        self.local_served.store(0, Ordering::Relaxed);
    }
}

/// Per-shard counters. Each shard owns one of these and mutates it with no LOCK (its single
/// backing [`ShardCountersCell`] is touched only by that core; the stores are uncontended
/// `Relaxed` atomics, the same single-increment the prior plain `u64` was, see the cell docs).
/// For INFO the server reads this shard's [`CounterSnapshot`]; for the out-of-band `/metrics`
/// endpoint the [`MetricsRegistry`] reads EVERY shard's cell across threads and sums them.
#[derive(Debug, Default, Clone)]
pub struct ShardCounters {
    cell: Arc<ShardCountersCell>,
}

/// A RAII guard for one PARKED blocking client (#661). [`ShardCounters::block_guard`] increments
/// the shard's `blocked_clients` gauge on construction; this drops it on EVERY normal park exit --
/// a wake, a timeout, or a client disconnect -- so INFO's node-wide `blocked_clients` reflects the
/// LIVE parked count and no exit path can leak it. The tokio serve loop holds one across the entire
/// block-park (the io_uring datapath does not truly park in v1 -- it replies immediately -- so it
/// has no parked client to count; a guard must be added there if it ever gains a real park).
/// Holds an owned `Arc` to the shard's cell, so it needs no borrow of the counters and its `Drop`
/// runs after the guard outlives the serve borrow. (On an unwinding panic the `Drop` also clears
/// the count; the shipping release profile is `panic = abort`, where the process ends and the live
/// count is moot regardless.)
pub struct BlockedGuard {
    cell: Arc<ShardCountersCell>,
}

impl Drop for BlockedGuard {
    fn drop(&mut self) {
        // Saturating decrement without a lock (mirrors `on_connection_close`): an increment always
        // precedes the guard on the SAME shard, so the gauge never underflows in practice, but the
        // saturating CAS guards against it anyway. Uncontended (the shard owns its cell), so the
        // loop runs once.
        let mut cur = self.cell.blocked_clients.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(1);
            match self.cell.blocked_clients.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

impl ShardCounters {
    /// A fresh zeroed counter set with its own backing cell.
    #[must_use]
    pub fn new() -> Self {
        ShardCounters::default()
    }

    /// Wrap an EXISTING backing cell (the one a [`MetricsRegistry`] pre-allocated for this
    /// shard's index), so the shard mutates the SAME cell the metrics task reads. The shard
    /// adopts its registry cell at boot via this constructor.
    #[must_use]
    pub fn with_cell(cell: Arc<ShardCountersCell>) -> Self {
        ShardCounters { cell }
    }

    /// A clone of the backing cell, for registering this shard in the [`MetricsRegistry`].
    #[must_use]
    pub fn cell(&self) -> Arc<ShardCountersCell> {
        Arc::clone(&self.cell)
    }

    /// Record a newly accepted connection.
    pub fn on_connection_open(&mut self) {
        self.cell
            .connections_received
            .fetch_add(1, Ordering::Relaxed);
        self.cell.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a closed connection. `connected_clients` is a gauge; a close always follows an
    /// open on the same shard, so the count never underflows, but we guard against it anyway
    /// with a CAS-free saturating decrement.
    pub fn on_connection_close(&mut self) {
        // Saturating decrement without a lock: load, subtract-or-clamp, compare-exchange-retry.
        // Uncontended (this shard owns the cell), so the loop runs once in practice.
        let mut cur = self.cell.connected_clients.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(1);
            match self.cell.connected_clients.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Increment this shard's `blocked_clients` gauge and return a [`BlockedGuard`] that decrements
    /// it on Drop (#661). The serve loop holds the guard across a whole block-park, so the gauge
    /// tracks the live parked count leak-free. `&self`: the gauge is a `Relaxed` atomic.
    #[must_use]
    pub fn block_guard(&self) -> BlockedGuard {
        self.cell.blocked_clients.fetch_add(1, Ordering::Relaxed);
        BlockedGuard {
            cell: Arc::clone(&self.cell),
        }
    }

    /// Record a processed command.
    pub fn on_command(&mut self) {
        self.cell.commands_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one command's elapsed micros into this shard's latency HISTOGRAM (#546). Takes
    /// `&self` (the underlying cell records through `Relaxed` atomics, no `&mut` needed), so the
    /// serve loop can observe latency without a mutable borrow of the counters. Reuses the same
    /// elapsed the SLOWLOG/COMMANDSTATS hooks measured -- no new clock read on the hot path.
    pub fn observe_latency(&self, elapsed_us: u64) {
        self.cell.observe_latency(elapsed_us);
    }

    /// Record `n` bytes READ off a client socket into this shard's `total_net_input_bytes` (#527):
    /// ONE uncontended `Relaxed` add, reached from every serve RECV site on BOTH datapaths (tokio +
    /// io_uring). Takes `&self` (the cell records through a `Relaxed` atomic, like
    /// [`Self::observe_latency`]) so the serve loop needs no `&mut` borrow; no lock, no allocation --
    /// the same single-increment the command counters compile to.
    pub fn on_net_input(&self, n: u64) {
        self.cell
            .total_net_input_bytes
            .fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` bytes WRITTEN to a client socket into this shard's `total_net_output_bytes`
    /// (#527): the SEND-path complement of [`Self::on_net_input`], one `Relaxed` add reached from
    /// every serve SEND site on both datapaths.
    pub fn on_net_output(&self, n: u64) {
        self.cell
            .total_net_output_bytes
            .fetch_add(n, Ordering::Relaxed);
    }

    /// Record one executed command into this shard's per-command COMMANDSTATS slot (#527): ONE
    /// `Relaxed` calls-increment + a usec add (+ a failed-increment on an error reply), the
    /// per-command analog of [`Self::observe_latency`]. `index` is the server's STABLE command
    /// ordinal; an out-of-range index (never produced by the server's ordinal map) is a defensive
    /// no-op. No lock, no allocation on the hot path -- the slot is pre-allocated.
    pub fn on_command_stat(&self, index: usize, usec: u64, failed: bool) {
        if let Some(slot) = self.cell.cmd_stats.get(index) {
            slot.record(usec, failed);
        }
    }

    /// Record that THIS shard DISPATCHED one single-target cross-shard keyed hop to a peer (#556):
    /// the hop it PAID (the #517 zero-hop harness's numerator). Bumped once on the coordinator's
    /// single-owner remote branch in `route_and_dispatch`. Takes `&self` (the cell records through a
    /// `Relaxed` atomic, like [`Self::observe_latency`]) so the serve loop needs no `&mut` borrow;
    /// ONE uncontended increment, no alloc, no clock -- the additive hot-path budget #556 allows.
    pub fn on_hop_sent(&self) {
        self.cell.hops_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that THIS shard SERVED one peer's cross-shard request off its inbox (#556): the drain
    /// loop ran a remote keyed / whole-keyspace unit for another shard. ONE `Relaxed` increment,
    /// reusing the drain loop's existing per-unit borrow.
    pub fn on_hop_served(&self) {
        self.cell.hops_served.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that THIS shard served one keyed request LOCALLY (#556): the request's owner IS the
    /// home shard, so it took the ZERO-hop fast path. The complement of [`Self::on_hop_sent`], so an
    /// operator/test derives hop-rate = `hops_sent / (hops_sent + local_served)`. ONE `Relaxed`
    /// increment on the existing home-serve branch.
    pub fn on_local_served(&self) {
        self.cell.local_served.fetch_add(1, Ordering::Relaxed);
    }

    /// Record `n` keys evicted to honor the memory ceiling (PR-3a; INFO
    /// `evicted_keys`). Called by the dispatch admission path after `evict_to_fit`.
    pub fn on_evicted(&mut self, n: u64) {
        self.cell.evicted_keys.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` keys reclaimed due to TTL expiry (PR-3b; INFO `expired_keys`).
    /// Called by the serve loop after the active timing-wheel drain.
    pub fn on_expired(&mut self, n: u64) {
        self.cell.expired_keys.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` keyspace hits (a read found a live key, INFO `keyspace_hits`).
    pub fn on_keyspace_hits(&mut self, n: u64) {
        self.cell.keyspace_hits.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` keyspace misses (a read found no live key, INFO `keyspace_misses`).
    pub fn on_keyspace_misses(&mut self, n: u64) {
        self.cell.keyspace_misses.fetch_add(n, Ordering::Relaxed);
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
            // The SERVING shard's per-shard reset (the same resettable set Redis clears). On a
            // multi-shard node the serve loop ALSO fans the reset across every OTHER shard's cell
            // via [`MetricsRegistry::reset_stats`], so the node-wide INFO rollup zeroes (#531).
            self.cell.reset_stats();
        }
        if d.evicted != 0 {
            self.cell
                .evicted_keys
                .fetch_add(d.evicted, Ordering::Relaxed);
        }
        if d.expired != 0 {
            self.cell
                .expired_keys
                .fetch_add(d.expired, Ordering::Relaxed);
        }
        if d.keyspace_hits != 0 {
            self.cell
                .keyspace_hits
                .fetch_add(d.keyspace_hits, Ordering::Relaxed);
        }
        if d.keyspace_misses != 0 {
            self.cell
                .keyspace_misses
                .fetch_add(d.keyspace_misses, Ordering::Relaxed);
        }
    }

    /// Take an immutable snapshot for rollup (reads this shard's backing cell).
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        self.cell.snapshot()
    }
}

/// The number of `(wall_millis, total_commands)` samples the ops/sec sampler retains (#549). Small:
/// the rate is read over the ring's oldest-to-newest span, so a handful of recent samples is enough
/// to smooth a single spiky read without diluting a sustained rate. Mirrors the spirit of Redis's
/// fixed instantaneous-metric ring.
const OPS_SAMPLE_RING: usize = 16;

/// One `(timestamp, total_commands)` sample the [`OpsPerSecSampler`] retains. The timestamp is
/// WALL-CLOCK milliseconds from the Env `Clock` seam (`now_unix_millis`, ADR-0003): unlike the
/// per-shard monotonic clock (whose origin differs per shard), the wall clock is comparable across
/// the shards that may each serve an INFO read into the SAME shared ring, and it is the same basis
/// Redis samples its instantaneous metrics on.
#[derive(Debug, Clone, Copy)]
struct OpsSample {
    /// Wall-clock milliseconds at which the sample was taken (`Clock::now_unix_millis`).
    at_millis: u64,
    /// The node-wide `commands_processed` total at that instant.
    total: u64,
}

/// The node-wide `instantaneous_ops_per_sec` sampler (#549): a small ring of
/// `(wall_millis, total_commands)` samples updated ON the (cold) INFO / metrics read, from which
/// INFO reports the recent commands-per-second rate. Previously that field was HARDWIRED to 0, so an
/// operator could not see load.
///
/// ## How the rate is computed
///
/// Each read appends `(now_millis, total_commands)` via [`Self::observe`] and returns the rate over
/// the retained window: `(newest.total - oldest.total) * 1000 / (newest.at - oldest.at)`. With a
/// single sample so far (or no elapsed time) the rate is 0. Under sustained load two reads a second
/// apart report the driven rate; sparse reads report the honest average over their span. The ring
/// bounds how far back "oldest" reaches ([`OPS_SAMPLE_RING`] samples).
///
/// ## Determinism + shared-nothing
///
/// Time comes ONLY from the Env `Clock` seam (ADR-0003); the sampler never reads the OS clock. It is
/// NODE-LEVEL cold state behind ONE standard-library mutex, taken only on the rare INFO/metrics read
/// (never the per-command hot path), so `bytes_per_key` and steady-state throughput are unaffected.
/// The lock is the sanctioned shared-nothing carve-out (mirroring [`ops`]).
#[derive(Debug, Default)]
pub struct OpsPerSecSampler {
    ring: Mutex<std::collections::VecDeque<OpsSample>>, // lint-allow: shared-nothing -- node-level cold state, taken only on the rare INFO/metrics read.
}

impl OpsPerSecSampler {
    /// A fresh sampler with an empty ring (first read reports 0 ops/sec until a second sample lands).
    #[must_use]
    pub fn new() -> Self {
        OpsPerSecSampler {
            ring: Mutex::new(std::collections::VecDeque::with_capacity(OPS_SAMPLE_RING)),
        }
    }

    /// Record a `(now_millis, total_commands)` sample and return the current
    /// `instantaneous_ops_per_sec` estimate over the retained window. `now_millis` is wall-clock
    /// milliseconds from the Env `Clock` seam (`now_unix_millis`); `total_commands` is the node-wide
    /// `commands_processed` total (summed across shards via [`MetricsRegistry::aggregate`]).
    ///
    /// Robust to a wall-clock that does not advance or steps backwards (a benign no-op / coalesce for
    /// this cold observability metric): a same-millisecond read updates the newest sample's total in
    /// place (no zero-duration division), and the rate uses `saturating_sub` on both axes.
    pub fn observe(&self, now_millis: u64, total_commands: u64) -> u64 {
        let Ok(mut ring) = self.ring.lock() else {
            // A poisoned lock (a prior panic while holding it) is not worth failing an INFO read over;
            // report 0 rather than propagate.
            return 0;
        };
        match ring.back_mut() {
            // Coalesce a same-instant (or backwards-clock) read into the newest sample so the window
            // never carries a zero/negative time step; keep the larger command total (monotonic).
            Some(last) if now_millis <= last.at_millis => {
                last.total = last.total.max(total_commands);
            }
            _ => {
                ring.push_back(OpsSample {
                    at_millis: now_millis,
                    total: total_commands,
                });
                if ring.len() > OPS_SAMPLE_RING {
                    ring.pop_front();
                }
            }
        }
        // Need two distinct-time samples to compute a rate.
        let (Some(oldest), Some(newest)) = (ring.front(), ring.back()) else {
            return 0;
        };
        let dt_ms = newest.at_millis.saturating_sub(oldest.at_millis);
        if dt_ms == 0 {
            return 0;
        }
        let dcommands = newest.total.saturating_sub(oldest.total);
        // commands over dt_ms milliseconds -> per-second rate (integer, matching Redis's field type).
        dcommands.saturating_mul(1000) / dt_ms
    }
}

/// The process-wide METRICS REGISTRY (OBSERVABILITY.md, #152): one [`ShardCountersCell`] per
/// shard, pre-allocated at boot and shared (by `Arc`) into BOTH the shard (which mutates its
/// own cell) AND the out-of-band `/metrics` HTTP task (which reads EVERY cell across threads
/// and sums them into a node-wide [`CounterSnapshot`]).
///
/// It is a lock-free aggregation point: there is NO `Mutex` (this crate is a hot-path crate;
/// shared-nothing ADR-0002), only an immutable `Vec` of `Arc<ShardCountersCell>` fixed at boot.
/// A shard ADOPTS its pre-allocated cell at its index via [`MetricsRegistry::shard_cell`]
/// (the registry pre-fills `shards` cells; the shard wraps cell `index` into its
/// [`ShardCounters`]). The registry is `Some` when the metrics endpoint is enabled -- by DEFAULT the
/// localhost bind (#555), or an explicit `--metrics-addr`; it is `None` only when the endpoint is
/// DISABLED (`--metrics-addr off`), in which case the shard's counters use a fresh standalone cell
/// (byte-identical to the prior default-off behavior).
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// One backing cell per shard, in shard-index order (`cells[i]` belongs to shard `i`).
    cells: Arc<Vec<Arc<ShardCountersCell>>>,
    /// The node-wide `instantaneous_ops_per_sec` sampler (#549), shared by `Arc` so every clone of
    /// the registry (one per shard's context) feeds the SAME ring on an INFO read. Node-level cold
    /// state; touched only on the rare INFO/metrics read, never per command.
    ops_rate: Arc<OpsPerSecSampler>,
}

impl MetricsRegistry {
    /// Pre-allocate one zeroed [`ShardCountersCell`] per shard. Called ONCE at boot when the
    /// metrics endpoint is enabled; the cells outlive every shard (held by the `Arc<Vec<_>>`
    /// the metrics task keeps).
    #[must_use]
    pub fn new(shards: usize) -> Self {
        let cells = (0..shards.max(1))
            .map(|_| Arc::new(ShardCountersCell::default()))
            .collect();
        MetricsRegistry {
            cells: Arc::new(cells),
            ops_rate: Arc::new(OpsPerSecSampler::new()),
        }
    }

    /// The node-wide `instantaneous_ops_per_sec` sampler (#549), for the INFO read to sample the
    /// node-wide command total against the Env clock and report the recent rate. Shared across every
    /// shard's registry clone, so any shard serving INFO feeds the same ring.
    #[must_use]
    pub fn ops_rate(&self) -> &OpsPerSecSampler {
        &self.ops_rate
    }

    /// The pre-allocated backing cell for shard `index`, for the shard to adopt into its
    /// [`ShardCounters`] at boot. A defensive modulo keeps an out-of-range index in bounds
    /// (a wiring bug clamps rather than panicking the shard thread); the registry is always
    /// sized to the shard count, so this is exact in practice.
    #[must_use]
    pub fn shard_cell(&self, index: usize) -> Arc<ShardCountersCell> {
        let n = self.cells.len().max(1);
        Arc::clone(&self.cells[index % n])
    }

    /// The number of registered shard cells.
    #[must_use]
    pub fn shards(&self) -> usize {
        self.cells.len()
    }

    /// Sum every shard's cell into ONE node-wide [`CounterSnapshot`] (the cross-shard rollup the
    /// `/metrics` endpoint and a future cross-shard INFO read). Lock-free: it loads each cell's
    /// atomics and folds them with [`CounterSnapshot::merge`].
    #[must_use]
    pub fn aggregate(&self) -> CounterSnapshot {
        self.cells
            .iter()
            .map(|c| c.snapshot())
            .fold(CounterSnapshot::default(), CounterSnapshot::merge)
    }

    /// Sum every shard's per-command COMMANDSTATS table into ONE node-wide per-slot vector (#527),
    /// the per-command analog of [`Self::aggregate`]: element-wise across shards so `out[ordinal]`
    /// is the whole node's calls/usec/failed for the command at that ordinal. Lock-free (each cell's
    /// slots are loaded independently). The SERVER maps the slot ordinals back to command names when
    /// it renders `cmdstat_<name>` for INFO COMMANDSTATS, so this crate stays name-agnostic. The
    /// result length is always [`COMMAND_STAT_SLOTS`]; unused ordinals stay zero.
    #[must_use]
    pub fn aggregate_command_stats(&self) -> Vec<CmdStat> {
        let mut out = vec![CmdStat::default(); COMMAND_STAT_SLOTS];
        for cell in self.cells.iter() {
            for (dst, src) in out.iter_mut().zip(cell.cmd_stats.iter()) {
                let s = src.snapshot();
                dst.calls = dst.calls.saturating_add(s.calls);
                dst.usec = dst.usec.saturating_add(s.usec);
                dst.rejected_calls = dst.rejected_calls.saturating_add(s.rejected_calls);
                dst.failed_calls = dst.failed_calls.saturating_add(s.failed_calls);
            }
        }
        out
    }

    /// One [`CounterSnapshot`] per shard, in shard-index order (`[i]` is shard `i`), for the
    /// per-shard labeled `/metrics` series (#362). Lock-free: each cell is loaded independently. The
    /// node rollup ([`Self::aggregate`]) is the sum of these.
    #[must_use]
    pub fn per_shard_snapshots(&self) -> Vec<CounterSnapshot> {
        self.cells.iter().map(|c| c.snapshot()).collect()
    }

    /// Sum every shard's latency histogram into ONE node-wide [`LatencyHistogram`] (#546), the
    /// element-wise cross-shard rollup the `/metrics` histogram series renders. Lock-free, mirroring
    /// [`Self::aggregate`]: it snapshots each cell's histogram and folds them with
    /// [`LatencyHistogram::merge`], so the node `+Inf`/`_count`/`_sum` are the sum of the per-shard
    /// values (the invariant the acceptance test checks).
    #[must_use]
    pub fn aggregate_latency(&self) -> LatencyHistogram {
        self.cells
            .iter()
            .map(|c| c.latency_snapshot())
            .fold(LatencyHistogram::default(), |acc, h| acc.merge(&h))
    }

    /// One [`LatencyHistogram`] per shard, in shard-index order (#546), for the per-shard labeled
    /// `/metrics` histogram series and for the test that checks the node rollup equals their sum.
    #[must_use]
    pub fn per_shard_latency(&self) -> Vec<LatencyHistogram> {
        self.cells.iter().map(|c| c.latency_snapshot()).collect()
    }

    /// `CONFIG RESETSTAT` NODE-WIDE (#531): zero the resettable stat counters of EVERY shard's
    /// cell, so an INFO rollup read through [`Self::aggregate`] reports zeroed totals. Since INFO's
    /// `# Stats`/`# Clients` counters are now the node-wide sum, a reset that touched only the
    /// SERVING shard would leave every sibling shard's stale totals in the rollup; this fans the
    /// reset across all cells. Lock-free (per-cell `Relaxed` stores); under concurrent traffic a
    /// racing increment on another shard may survive, which is the inherent, acceptable fuzziness
    /// of a stats reset on a sharded node (there is no global stop-the-world).
    pub fn reset_stats(&self) {
        for c in self.cells.iter() {
            c.reset_stats();
        }
    }
}

/// The raft-mode control-plane gauges the `/metrics` endpoint exposes (HA-4c), read by the
/// binary's metrics task from its `RaftHandle` snapshot. `None` outside raft-governance mode
/// (the DEFAULT static path), in which case the renderer omits the `ironcache_raft_*` series
/// entirely (a standalone node has no raft state to report).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftGauges {
    /// `true` iff this node currently believes it is the Raft leader.
    pub is_leader: bool,
    /// The node's persisted current term.
    pub current_term: u64,
    /// The highest log index known committed.
    pub commit_index: u64,
    /// The size of the current VOTER set (counted in every election + commit quorum).
    pub voters: u64,
}

/// The process-level GAUGES the `/metrics` endpoint exposes alongside the aggregated counters:
/// the figures that are NOT per-shard counters (uptime, the process-global allocator memory, and
/// the optional raft control-plane state). The binary reads each at scrape time (uptime via the
/// Env clock seam, memory via the store's jemalloc mallctl, raft via the `RaftHandle` snapshot)
/// and hands them to [`render_prometheus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsGauges {
    /// Seconds since the process started serving (from the Env monotonic clock).
    pub uptime_secs: u64,
    /// The configured shard count.
    pub shards: u64,
    /// `used_memory`: the allocator-attributed live allocated total in bytes (ADR-0006).
    pub used_memory: u64,
    /// `used_memory_rss`: the resident set size in bytes.
    pub used_memory_rss: u64,
    /// The current effective `maxmemory` ceiling in bytes (`0` = unlimited).
    pub maxmemory: u64,
    /// Unix seconds of the last successful save (`0` when persistence is off / no save yet).
    pub last_save_unix: u64,
    /// The persistence DIRTY counter (changes since the last save; `0` when persistence off).
    pub rdb_changes_since_save: u64,
    /// REPLICATION (#549): whether this node's replication link is healthy. A REPLICA reports the
    /// link to its master (`master_link_status:up`); a MASTER / standalone reports `true` (it has no
    /// upstream link to lose). Rendered as `ironcache_replication_link_up 1|0` so a replica losing
    /// its master is visible on a scrape.
    pub repl_link_up: bool,
    /// REPLICATION (#549): the replication lag in LOGICAL WRITE OFFSETS (IronCache tracks a logical
    /// write offset, not a time delta, so the honest unit is offsets, not seconds). A REPLICA reports
    /// its own lag (`master_offset - node_offset`); a MASTER reports its worst connected replica's
    /// lag; `0` when caught up, standalone, or the link is down (unknown). Rendered as
    /// `ironcache_replication_lag_offset`.
    pub repl_lag_offset: u64,
    /// The raft control-plane gauges, `Some` only in raft-governance mode.
    pub raft: Option<RaftGauges>,
}

/// Render the Prometheus text exposition (version 0.0.4) for `GET /metrics`: `# HELP`/`# TYPE`
/// headers followed by `name value` samples, one metric family at a time, using the stable
/// `ironcache_<subsystem>_<name>` naming. `counters` is the node-wide rollup (summed across every
/// shard via [`MetricsRegistry::aggregate`]); `gauges` carries the process-level figures.
///
/// The body is plain ASCII, deterministic, and self-consistent: each family emits its HELP/TYPE
/// once then its sample(s). The raft families are emitted ONLY when `gauges.raft` is `Some`
/// (a standalone node reports no `ironcache_raft_*` series). The caller serves this with
/// `Content-Type: text/plain; version=0.0.4`.
///
/// `too_many_lines` is allowed: this is one flat list of metric families (each a HELP/TYPE +
/// sample), the single place the exposition is rendered. Splitting it would scatter the metric
/// catalog across helpers with no readability gain; the body is linear and obvious.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn render_prometheus(counters: CounterSnapshot, gauges: MetricsGauges) -> String {
    use core::fmt::Write as _;
    let mut o = String::with_capacity(2048);

    // A counter family: HELP + TYPE counter + one sample.
    let mut counter = |name: &str, help: &str, value: u64| {
        let _ = write!(
            o,
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
        );
    };
    counter(
        "ironcache_connections_received_total",
        "Connections accepted since start.",
        counters.connections_received,
    );
    counter(
        "ironcache_commands_processed_total",
        "Commands processed since start.",
        counters.commands_processed,
    );
    counter(
        "ironcache_evicted_keys_total",
        "Keys evicted to honor the memory ceiling.",
        counters.evicted_keys,
    );
    counter(
        "ironcache_expired_keys_total",
        "Keys reclaimed because their TTL passed.",
        counters.expired_keys,
    );
    counter(
        "ironcache_keyspace_hits_total",
        "Read commands that found a live key.",
        counters.keyspace_hits,
    );
    counter(
        "ironcache_keyspace_misses_total",
        "Read commands that found no live key.",
        counters.keyspace_misses,
    );
    // COORDINATOR HOP counters (#556, the #517 zero-hop measurement harness): how much cross-shard
    // hopping the node is doing, and the no-hop path it takes. hop-rate = hops_sent / (hops_sent +
    // local_served); in shard-owners mode with an owner-dialing client `hops_sent` trends to ~0.
    counter(
        "ironcache_hops_sent_total",
        "Cross-shard requests dispatched to a peer shard (the hop paid).",
        counters.hops_sent,
    );
    counter(
        "ironcache_hops_served_total",
        "Cross-shard requests received and served for a peer shard.",
        counters.hops_served,
    );
    counter(
        "ironcache_local_served_total",
        "Keyed requests served locally (owner is the home shard, no hop).",
        counters.local_served,
    );

    // A gauge family: HELP + TYPE gauge + one sample.
    let mut gauge = |name: &str, help: &str, value: u64| {
        let _ = write!(
            o,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
        );
    };
    gauge(
        "ironcache_connected_clients",
        "Currently-open client connections.",
        counters.connected_clients,
    );
    gauge(
        "ironcache_blocked_clients",
        "Clients currently parked on a blocking command (BLPOP/BRPOP/BLMOVE/WAIT).",
        counters.blocked_clients,
    );
    gauge(
        "ironcache_keyspace_keys",
        "Live keys held across all shards and databases.",
        counters.keyspace_keys,
    );
    gauge(
        "ironcache_uptime_seconds",
        "Seconds since the process started serving.",
        gauges.uptime_secs,
    );
    gauge(
        "ironcache_shards",
        "Configured shard (thread-per-core) count.",
        gauges.shards,
    );
    gauge(
        "ironcache_used_memory_bytes",
        "Allocator-attributed live allocated bytes (jemalloc stats.allocated).",
        gauges.used_memory,
    );
    gauge(
        "ironcache_used_memory_rss_bytes",
        "Resident set size in bytes (jemalloc stats.resident).",
        gauges.used_memory_rss,
    );
    gauge(
        "ironcache_maxmemory_bytes",
        "Effective maxmemory ceiling in bytes (0 means unlimited).",
        gauges.maxmemory,
    );
    gauge(
        "ironcache_persistence_last_save_unixtime",
        "Unix seconds of the last successful save (0 when persistence is off).",
        gauges.last_save_unix,
    );
    gauge(
        "ironcache_persistence_rdb_changes_since_save",
        "Changes since the last save (the dirty counter; 0 when persistence is off).",
        gauges.rdb_changes_since_save,
    );

    // REPLICATION gauges (#549): a replica falling behind (or losing its master) is otherwise
    // invisible. `link_up` is 1 when this node's replication link is healthy (a replica's link to its
    // master; a master/standalone is trivially up); `lag_offset` is the lag in logical write offsets.
    gauge(
        "ironcache_replication_link_up",
        "1 when this node's replication link is up (a replica's link to its master; a master is 1).",
        u64::from(gauges.repl_link_up),
    );
    gauge(
        "ironcache_replication_lag_offset",
        "Replication lag in logical write offsets (a replica's own lag; a master's worst replica; 0 when caught up / standalone / link down).",
        gauges.repl_lag_offset,
    );

    if let Some(r) = gauges.raft {
        gauge(
            "ironcache_raft_is_leader",
            "1 when this node currently believes it is the Raft leader, else 0.",
            u64::from(r.is_leader),
        );
        gauge(
            "ironcache_raft_current_term",
            "The node's persisted current Raft term.",
            r.current_term,
        );
        gauge(
            "ironcache_raft_commit_index",
            "The highest Raft log index known committed.",
            r.commit_index,
        );
        gauge(
            "ironcache_raft_voters",
            "The size of the current Raft voter set.",
            r.voters,
        );
    }

    o
}

/// Emit ONE per-shard metric family into `o`: the `# HELP`/`# TYPE` headers once, then one
/// `{shard="i"}`-labeled sample per shard (the value picked from each shard's snapshot by `pick`).
fn shard_family(
    o: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    per_shard: &[CounterSnapshot],
    pick: impl Fn(&CounterSnapshot) -> u64,
) {
    use core::fmt::Write as _;
    let _ = write!(o, "# HELP {name} {help}\n# TYPE {name} {kind}\n");
    for (i, s) in per_shard.iter().enumerate() {
        let _ = writeln!(o, "{name}{{shard=\"{i}\"}} {}", pick(s));
    }
}

/// Render the PER-SHARD labeled `/metrics` series (#362): for each per-shard counter/gauge, an
/// `ironcache_shard_<name>{shard="i"}` family carrying that shard's value, so the console can render
/// shard-level (thread-per-core) views in addition to the node rollup.
///
/// This is ADDITIVE and uses a DISTINCT `ironcache_shard_*` namespace, so the node-rollup families
/// from [`render_prometheus`] are byte-unchanged and there is no mixed-label double-count within a
/// family. Only the counters/gauges with genuine per-shard meaning are labeled; process-global
/// gauges (uptime, allocator memory, raft) stay node-level in [`render_prometheus`]. The caller
/// appends this to the rollup body; with one shard each family carries a single `shard="0"` sample.
#[must_use]
pub fn render_prometheus_shards(per_shard: &[CounterSnapshot]) -> String {
    let mut o = String::with_capacity(per_shard.len() * 256 + 256);
    shard_family(
        &mut o,
        "ironcache_shard_connections_received_total",
        "Connections accepted since start, per shard.",
        "counter",
        per_shard,
        |s| s.connections_received,
    );
    shard_family(
        &mut o,
        "ironcache_shard_commands_processed_total",
        "Commands processed since start, per shard.",
        "counter",
        per_shard,
        |s| s.commands_processed,
    );
    shard_family(
        &mut o,
        "ironcache_shard_evicted_keys_total",
        "Keys evicted to honor the memory ceiling, per shard.",
        "counter",
        per_shard,
        |s| s.evicted_keys,
    );
    shard_family(
        &mut o,
        "ironcache_shard_expired_keys_total",
        "Keys reclaimed because their TTL passed, per shard.",
        "counter",
        per_shard,
        |s| s.expired_keys,
    );
    shard_family(
        &mut o,
        "ironcache_shard_keyspace_hits_total",
        "Read commands that found a live key, per shard.",
        "counter",
        per_shard,
        |s| s.keyspace_hits,
    );
    shard_family(
        &mut o,
        "ironcache_shard_keyspace_misses_total",
        "Read commands that found no live key, per shard.",
        "counter",
        per_shard,
        |s| s.keyspace_misses,
    );
    // COORDINATOR HOP counters, per shard (#556): the shard-level view of the hop / no-hop split the
    // #517 zero-hop harness reads (node-wide equivalents live in `render_prometheus`).
    shard_family(
        &mut o,
        "ironcache_shard_hops_sent_total",
        "Cross-shard requests dispatched to a peer shard (the hop paid), per shard.",
        "counter",
        per_shard,
        |s| s.hops_sent,
    );
    shard_family(
        &mut o,
        "ironcache_shard_hops_served_total",
        "Cross-shard requests received and served for a peer shard, per shard.",
        "counter",
        per_shard,
        |s| s.hops_served,
    );
    shard_family(
        &mut o,
        "ironcache_shard_local_served_total",
        "Keyed requests served locally (owner is the home shard, no hop), per shard.",
        "counter",
        per_shard,
        |s| s.local_served,
    );
    shard_family(
        &mut o,
        "ironcache_shard_connected_clients",
        "Currently-open client connections, per shard.",
        "gauge",
        per_shard,
        |s| s.connected_clients,
    );
    shard_family(
        &mut o,
        "ironcache_shard_keyspace_keys",
        "Live keys held, per shard.",
        "gauge",
        per_shard,
        |s| s.keyspace_keys,
    );
    o
}

/// Render the cross-shard INBOX-DEPTH gauge (#556): the node-wide `ironcache_inbox_depth` (the sum
/// of every shard's queued cross-shard work) plus the per-shard `ironcache_shard_inbox_depth{shard}`
/// occupancy, so an operator sees back-pressure BUILDING before the 1024-bounded inbox stalls a home
/// core. `depths[i]` is shard `i`'s current inbox occupancy.
///
/// Unlike the hop COUNTERS (which live in the [`MetricsRegistry`] cells), the depth is SAMPLED from
/// the mpsc channel length at scrape time (the caller reads `max_capacity - capacity` per shard, see
/// `coordinator::inbox_depths`), NOT tracked with a per-enqueue/dequeue atomic -- so this gauge costs
/// the cross-shard hop path NOTHING (no new atomic, no cross-core write on the send/drain branch).
/// The metrics endpoint passes an all-zero slice sized to the shard count when no coordinator inbox
/// is wired (a degenerate unit-test state), so the series always appears.
#[must_use]
pub fn render_inbox_depth(depths: &[u64]) -> String {
    use core::fmt::Write as _;
    let mut o = String::with_capacity(depths.len() * 48 + 128);
    let node: u64 = depths.iter().copied().sum();
    let _ = write!(
        o,
        "# HELP ironcache_inbox_depth Cross-shard inbox occupancy (queued cross-shard work items) across all shards.\n\
         # TYPE ironcache_inbox_depth gauge\n\
         ironcache_inbox_depth {node}\n"
    );
    let _ = write!(
        o,
        "# HELP ironcache_shard_inbox_depth Cross-shard inbox occupancy (queued cross-shard work items), per shard.\n\
         # TYPE ironcache_shard_inbox_depth gauge\n"
    );
    for (i, d) in depths.iter().enumerate() {
        let _ = writeln!(o, "ironcache_shard_inbox_depth{{shard=\"{i}\"}} {d}");
    }
    o
}

/// The stable metric-family NAME for the command-latency histogram (#546). Prometheus derives the
/// `_bucket` / `_sum` / `_count` child series from this base, and the base unit is SECONDS (the
/// `_seconds` suffix), matching the Prometheus base-unit convention for a latency histogram.
const LATENCY_HISTOGRAM_NAME: &str = "ironcache_command_duration_seconds";

/// Append ONE histogram family sample-set to `o` under `name` from `hist`: the CUMULATIVE
/// `_bucket{le="..."}` series (Prometheus `le` semantics; monotonic non-decreasing, and the `+Inf`
/// bucket equals `_count`), then `_sum` (seconds) and `_count`. `shard` is `None` for the node
/// rollup (unlabeled samples) or `Some(i)` for the per-shard series (a `shard="i"` label), so the
/// two callers share the exact same bucket/sum/count format. Caller emits the `# HELP`/`# TYPE`.
fn write_histogram_family(
    o: &mut String,
    name: &str,
    shard: Option<usize>,
    hist: &LatencyHistogram,
) {
    use core::fmt::Write as _;
    let cumulative = hist.cumulative();
    // On a bucket line the shard label (when present) precedes the REQUIRED `le` label.
    let shard_label = shard.map(|i| format!("shard=\"{i}\",")).unwrap_or_default();
    for (le, cum) in LATENCY_BUCKET_LE.iter().zip(cumulative.iter()) {
        let _ = writeln!(o, "{name}_bucket{{{shard_label}le=\"{le}\"}} {cum}");
    }
    // `_sum`/`_count` carry only the shard label (no `le`), or NO braces at all for the node rollup.
    // `_sum` is the total observed latency in SECONDS; the micros total divides exactly by 1e6, so
    // six decimals reproduce it losslessly (the sub-microsecond digit is always zero).
    let sc_label = shard
        .map(|i| format!("{{shard=\"{i}\"}}"))
        .unwrap_or_default();
    let _ = writeln!(o, "{name}_sum{sc_label} {:.6}", hist.sum_seconds());
    let _ = writeln!(o, "{name}_count{sc_label} {}", hist.count);
}

/// Render the NODE-WIDE command-latency histogram (#546) as a Prometheus `histogram` family:
/// `ironcache_command_duration_seconds_{bucket,sum,count}`. `hist` is the cross-shard rollup
/// ([`MetricsRegistry::aggregate_latency`]); the caller appends this to the `/metrics` body. The
/// buckets are CUMULATIVE and monotonic non-decreasing, the `+Inf` bucket equals `_count`, and
/// `p99`/`p99.9` are derivable from the `le` series -- the operator-facing tail-latency view (#546).
#[must_use]
pub fn render_latency_histogram(hist: &LatencyHistogram) -> String {
    use core::fmt::Write as _;
    let mut o = String::with_capacity(1024);
    let name = LATENCY_HISTOGRAM_NAME;
    let _ = write!(
        o,
        "# HELP {name} Command execution latency in seconds (all commands, all shards).\n\
         # TYPE {name} histogram\n"
    );
    write_histogram_family(&mut o, name, None, hist);
    o
}

/// Render the PER-SHARD labeled command-latency histogram series (#546), mirroring
/// [`render_prometheus_shards`]: one `ironcache_shard_command_duration_seconds` histogram family per
/// shard, each `le`/`sum`/`count` sample carrying a `shard="i"` label. Additive to the node rollup
/// (a DISTINCT `ironcache_shard_*` namespace, so there is no mixed-label double-count within the
/// node family); the caller appends it to the `/metrics` body.
#[must_use]
pub fn render_latency_histogram_shards(per_shard: &[LatencyHistogram]) -> String {
    use core::fmt::Write as _;
    let mut o = String::with_capacity(per_shard.len() * 512 + 128);
    let name = "ironcache_shard_command_duration_seconds";
    let _ = write!(
        o,
        "# HELP {name} Command execution latency in seconds, per shard.\n\
         # TYPE {name} histogram\n"
    );
    for (i, hist) in per_shard.iter().enumerate() {
        write_histogram_family(&mut o, name, Some(i), hist);
    }
    o
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
    /// Connections currently PARKED on a blocking command on this shard (#661); summed across
    /// shards for the node-wide INFO `blocked_clients`.
    pub blocked_clients: u64,
    /// Total bytes READ off this shard's client sockets since start (INFO `total_net_input_bytes`,
    /// #527); summed across shards for the node-wide figure `redis_exporter` reads as
    /// `redis_net_input_bytes_total`.
    pub total_net_input_bytes: u64,
    /// Total bytes WRITTEN to this shard's client sockets since start (INFO
    /// `total_net_output_bytes`, #527); the node-wide `redis_net_output_bytes_total` source.
    pub total_net_output_bytes: u64,
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
    /// This shard's live KEY COUNT (a GAUGE, not a since-start total). Disjoint across shards
    /// (each shard owns its own keyspace partition), so [`CounterSnapshot::merge`] SUMS it into
    /// the node-wide key count for the `/metrics` keyspace gauge. Published off the command hot
    /// path (the periodic expiry tick), so it is eventually-consistent.
    pub keyspace_keys: u64,
    /// Cross-shard requests this shard DISPATCHED to a peer (the hop it paid), #556.
    pub hops_sent: u64,
    /// Cross-shard requests this shard RECEIVED + served for a peer (its drain loop), #556.
    pub hops_served: u64,
    /// Keyed requests this shard served LOCALLY (owner == home, no hop), #556 -- the complement of
    /// `hops_sent`, so hop-rate = `hops_sent / (hops_sent + local_served)`.
    pub local_served: u64,
}

impl CounterSnapshot {
    /// Fold another snapshot into this one (the rollup operation).
    #[must_use]
    pub fn merge(self, other: CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            connections_received: self.connections_received + other.connections_received,
            commands_processed: self.commands_processed + other.commands_processed,
            connected_clients: self.connected_clients + other.connected_clients,
            blocked_clients: self.blocked_clients + other.blocked_clients,
            total_net_input_bytes: self.total_net_input_bytes + other.total_net_input_bytes,
            total_net_output_bytes: self.total_net_output_bytes + other.total_net_output_bytes,
            evicted_keys: self.evicted_keys + other.evicted_keys,
            expired_keys: self.expired_keys + other.expired_keys,
            keyspace_hits: self.keyspace_hits + other.keyspace_hits,
            keyspace_misses: self.keyspace_misses + other.keyspace_misses,
            keyspace_keys: self.keyspace_keys + other.keyspace_keys,
            hops_sent: self.hops_sent + other.hops_sent,
            hops_served: self.hops_served + other.hops_served,
            local_served: self.local_served + other.local_served,
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
    /// The 40-lowercase-hex process RUN ID (INFO `run_id`), generated ONCE at boot through the
    /// determinism seam (ADR-0003: drawn from the binary's `SystemEnv` RNG in `serve::run_server`,
    /// then leaked to `'static`), identical across shards. UNLIKE `cluster_node_id` (a stable
    /// identity that in cluster mode is the configured announce id), the run id is ALWAYS a fresh
    /// per-boot random value: it identifies THIS process incarnation, so it changes on every restart
    /// (Redis parity -- clients + `redis_exporter` read `run_id` to detect a restart / distinguish
    /// incarnations). It is NOT the placeholder zero string it used to be.
    pub run_id: &'static str,
    /// Whether the server booted in cluster mode (Redis `cluster-enabled`,
    /// CLUSTER_CONTRACT.md #70). Reported by the INFO `# Cluster` section
    /// (`cluster_enabled:0/1`) and `CLUSTER INFO`. Slice 1 is cluster-disabled, so this is
    /// `false` in practice; the field is sourced from config so a later slice can flip it. It ALSO
    /// drives the INFO `# Server` `redis_mode` field (`cluster` when set, else `standalone`).
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

/// The extra runtime facts the INFO `# Clients` / `# Stats` / `# CPU` sections render (PROD-7
/// completeness), read by the caller from the runtime overlay + the connection gate so they
/// reflect a live `CONFIG SET` and the real connection count. A PLAIN POD so `ironcache-observe`
/// stays a leaf (the server crate fills it in). All zeros is a valid baseline (the default test
/// path), so [`Default`] gives the byte-compatible "no extra facts" rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeStats {
    /// `maxclients`: the effective simultaneous-connection ceiling (`0` = unlimited).
    pub maxclients: u64,
    /// `blocked_clients`: connections currently parked on a blocking command
    /// (BLPOP/BRPOP/BLMOVE/WAIT, ...), node-wide (#661). Sourced from the summed per-shard
    /// `blocked_clients` gauge (see [`CounterSnapshot::blocked_clients`]); the dispatch layer sets
    /// it from `MetricsRegistry::aggregate`.
    pub blocked_clients: u64,
    /// `instantaneous_ops_per_sec`: a coarse commands-per-second estimate (the rolling delta the
    /// caller computes off the per-shard `commands_processed` total); `0` before the first sample.
    pub instantaneous_ops_per_sec: u64,
    /// `rejected_connections`: connections refused by the `maxclients` gate since boot.
    pub rejected_connections: u64,
}

/// The facts the INFO `# Persistence` section renders (durability footgun fix #5), mirroring Redis
/// `rdb_*` field names so dashboards / `redis_exporter` parse them. Filled by the caller from the
/// node-level persistence state (last-save time + dirty counter) and the runtime save policy. A
/// node with persistence OFF passes `enabled: false`, which still renders a HONEST section (a cache
/// with no on-disk snapshot: `rdb_last_save_time:0`, an empty save policy) rather than omitting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistenceInfo {
    /// Whether durable persistence is enabled (a `data_dir` is configured). When `false` the
    /// section reports the no-snapshot posture (last-save 0, no changes, empty policy).
    pub enabled: bool,
    /// `rdb_last_save_time`: unix seconds of the last successful save (seeded on boot from the
    /// loaded snapshot's manifest, durability fix #2). `0` when nothing has been saved/loaded.
    pub rdb_last_save_time: u64,
    /// `rdb_changes_since_last_save`: keyspace writes since the last save (the dirty counter).
    pub rdb_changes_since_last_save: u64,
    /// `rdb_last_bgsave_status`: whether the last save COMMITTED (`true` -> `ok`, `false` -> `err`),
    /// #549. `true` before any save has run (Redis parity) and when persistence is off, so the
    /// canonical "last save failed" alert reads a real signal only after a genuine failure.
    pub last_bgsave_ok: bool,
    /// The SECONDS half of the active save point (the periodic cadence). `0` = the periodic save
    /// is OFF (only an explicit SAVE/BGSAVE persists), rendered as an EMPTY `save` policy.
    pub save_interval_secs: u64,
    /// The CHANGES half of the active save point. Meaningful only when `save_interval_secs > 0`.
    pub save_min_changes: u64,
}

/// The LIVE node-level persistence runtime stats shared into [`build_info`]'s INFO `# Persistence`
/// section (and the `/metrics` gauges): the last-save unix time and the dirty (changes-since-save)
/// counter, as two lock-free atomics. ONE per node, shared by `Arc`; the binary's persistence state
/// owns the writes (it stamps the last-save time on a committed save / seeds it on boot, and bumps
/// dirty per write), and the server crate's INFO path reads it lock-free. Defined HERE (not in the
/// binary) so `ServerContext` -- which lives in the server crate, below the binary -- can hold it
/// without an upward dependency. `None` on the persistence-OFF default path.
#[derive(Debug)]
pub struct PersistRuntime {
    last_save_unix_secs: AtomicU64,
    dirty: AtomicU64,
    /// Whether the most recent save COMMITTED successfully (#549). Defaults to `true` (Redis reports
    /// `rdb_last_bgsave_status:ok` before any save has run); a FAILED save flips it to `false` and a
    /// committed save flips it back to `true`. Read (relaxed) by the INFO `# Persistence` render so
    /// the canonical "last save failed" alert can be written; a lock-free node-level cell.
    last_bgsave_ok: AtomicBool,
}

impl Default for PersistRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistRuntime {
    /// A fresh persistence-runtime cell (last-save 0, dirty 0).
    #[must_use]
    pub fn new() -> Self {
        PersistRuntime {
            last_save_unix_secs: AtomicU64::new(0),
            dirty: AtomicU64::new(0),
            // Redis reports `rdb_last_bgsave_status:ok` before any save has run.
            last_bgsave_ok: AtomicBool::new(true),
        }
    }

    /// The last-save unix seconds (what INFO `rdb_last_save_time` / `LASTSAVE` report), relaxed.
    #[must_use]
    pub fn last_save_unix_secs(&self) -> u64 {
        self.last_save_unix_secs.load(Ordering::Relaxed)
    }

    /// Store the last-save unix seconds (a committed save, or the boot seed from the manifest).
    pub fn set_last_save_unix_secs(&self, secs: u64) {
        self.last_save_unix_secs.store(secs, Ordering::Relaxed);
    }

    /// The dirty (changes-since-last-save) counter, relaxed.
    #[must_use]
    pub fn dirty(&self) -> u64 {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Bump the dirty counter by one (a successful write while persistence is enabled), relaxed.
    pub fn note_write(&self) {
        self.dirty.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset the dirty counter to zero (a committed save), relaxed.
    pub fn reset_dirty(&self) {
        self.dirty.store(0, Ordering::Relaxed);
    }

    /// Whether the last save COMMITTED successfully (INFO `rdb_last_bgsave_status:ok|err`, #549),
    /// relaxed. `true` before any save has run (Redis parity).
    #[must_use]
    pub fn last_bgsave_ok(&self) -> bool {
        self.last_bgsave_ok.load(Ordering::Relaxed)
    }

    /// Record the outcome of a save (#549): `true` on a committed save, `false` on a failed one.
    /// Relaxed; the persistence save path calls this so INFO's `rdb_last_bgsave_status` is honest.
    pub fn set_last_bgsave_ok(&self, ok: bool) {
        self.last_bgsave_ok.store(ok, Ordering::Relaxed);
    }
}

/// A process-GLOBAL live-connection counter + ceiling (PROD-SAFETY #3, the `maxclients`
/// connection-exhaustion DoS fix). ONE per node, shared by `Arc` onto every shard's accept path.
///
/// ## Why it exists
///
/// The accept loop previously NEVER rejected a connection, so an attacker (or a misbehaving
/// client pool) could open unlimited connections and exhaust file descriptors / memory. This gate
/// tracks the live connection count and lets the accept path REJECT a new connection once the
/// count is at the configured `maxclients` ceiling, matching Redis's `-ERR max number of clients
/// reached`. The per-shard `connected_clients` metric is a separate per-shard gauge (for INFO /
/// `/metrics`); this is the ONE process-global count the cap is enforced against, because the cap
/// is a NODE-level limit, not a per-shard one.
///
/// ## Cost
///
/// One relaxed atomic `fetch_add` on accept (a COLD path: once per connection, not per command)
/// and one relaxed `fetch_sub` on close. The ceiling is read from the runtime overlay
/// (`maxclients`) on accept; `0` disables the cap (unlimited, the pre-fix behavior). The count is
/// shared-nothing-friendly: it is a single atomic, never a lock, and never touched per command.
#[derive(Debug, Default)]
pub struct ConnectionGate {
    live: AtomicU64,
    /// Connections REFUSED by the cap since boot (INFO `rejected_connections`, PROD-7). A monotonic
    /// counter bumped on each `try_admit` that returns `false`; read off the hot path (INFO render).
    rejected: AtomicU64,
}

impl ConnectionGate {
    /// A fresh gate with zero live connections.
    #[must_use]
    pub fn new() -> Self {
        ConnectionGate::default()
    }

    /// Try to ADMIT a new connection against the `maxclients` ceiling (PROD-SAFETY #3). When
    /// `maxclients == 0` the cap is disabled and this ALWAYS admits (incrementing the live count),
    /// the pre-fix behavior. Otherwise it admits iff the current live count is BELOW `maxclients`:
    /// on admit it increments and returns `true`; at/over the cap it returns `false` WITHOUT
    /// incrementing (the caller writes `-ERR max number of clients reached` and closes the socket).
    ///
    /// The check + increment is a single CAS loop so two concurrent accepts (across shards) can
    /// never both squeeze past the cap. Uncontended in practice (accepts are rare relative to
    /// commands), so the loop runs once.
    pub fn try_admit(&self, maxclients: u64) -> bool {
        if maxclients == 0 {
            // Cap disabled: admit unconditionally, but still track the live count so a later
            // `CONFIG SET maxclients` enforces against an accurate number.
            self.live.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let mut cur = self.live.load(Ordering::Relaxed);
        loop {
            if cur >= maxclients {
                // Over the cap: count the refusal (INFO `rejected_connections`) and reject.
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            match self.live.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => cur = observed,
            }
        }
    }

    /// The number of connections REFUSED by the cap since boot (INFO `rejected_connections`,
    /// PROD-7). A single relaxed load, read off the hot path.
    #[must_use]
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Record a connection close: decrement the live count (a saturating relaxed `fetch_sub` that
    /// never underflows). Called once per ADMITTED connection when it ends (a rejected connection
    /// was never counted, so it is NOT released here).
    pub fn release(&self) {
        let mut cur = self.live.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(1);
            match self
                .live
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// The current live connection count (a single relaxed load). For tests / introspection.
    #[must_use]
    pub fn live(&self) -> u64 {
        self.live.load(Ordering::Relaxed)
    }
}

/// A process-GLOBAL allocator-memory gauge (PROD-SAFETY #1/#2): the latest jemalloc
/// `stats.allocated` (the `used_memory` analog) and `stats.resident` (RSS) figures, as two
/// lock-free atomics. ONE per node, shared by `Arc` onto every shard's server context.
///
/// ## Why it exists (the host-OOM fix)
///
/// The maxmemory eviction trigger previously compared a per-shard LOGICAL key+value byte
/// counter against the per-shard budget. The logical figure UNDERCOUNTS true process memory
/// (slab slack, table overhead) by roughly 2x, so a node configured `maxmemory 4gb` could use
/// ~8gb RSS and OOM-kill the host: the ceiling protected a fiction, not the host. This gauge
/// carries the REAL allocator figure (the same one INFO `used_memory` / `used_memory_rss`
/// report) into the admission gate so the over-limit DECISION is driven off actual process
/// memory, and against the FULL `maxmemory` (a PROCESS-GLOBAL trigger, so a HOT shard sheds
/// even when its even-split per-shard budget is not individually exceeded -- PROD-SAFETY #2).
///
/// ## Why this is NOT new per-command cost
///
/// The allocator figure is read by the binary (which can call the jemalloc mallctl) OFF the
/// command hot path -- on the periodic active-expiry tick -- and PUBLISHED here with one
/// relaxed atomic store. The admission gate then reads it with one relaxed atomic LOAD on the
/// eviction path (only for a `denyoom` write while the ceiling is enabled), never advancing the
/// jemalloc epoch per command. `0` (the seed, and the MSVC/system-allocator fallback) means
/// "no allocator figure available", in which case the gate falls back to the per-shard logical
/// counter so the default/test behavior is byte-unchanged.
#[derive(Debug, Default)]
pub struct ProcessMemoryGauge {
    used_memory: AtomicU64,
    used_memory_rss: AtomicU64,
}

impl ProcessMemoryGauge {
    /// A fresh gauge seeded to 0 (no allocator figure read yet). Until the first publish the
    /// admission gate falls back to the per-shard logical counter (byte-unchanged default).
    #[must_use]
    pub fn new() -> Self {
        ProcessMemoryGauge::default()
    }

    /// Publish the latest allocator `(used_memory, used_memory_rss)` pair (two relaxed stores).
    /// Called by the binary OFF the command hot path (the periodic expiry tick reads the jemalloc
    /// mallctl once per cycle and stores here), so the per-command path never advances the epoch.
    pub fn publish(&self, used_memory: u64, used_memory_rss: u64) {
        self.used_memory.store(used_memory, Ordering::Relaxed);
        self.used_memory_rss
            .store(used_memory_rss, Ordering::Relaxed);
    }

    /// The latest published allocator `used_memory` (jemalloc `stats.allocated`) figure in bytes,
    /// a single relaxed load. `0` means no figure has been published yet (or the build has no
    /// allocator to query), and the admission gate treats `0` as "fall back to the logical counter".
    #[must_use]
    pub fn used_memory(&self) -> u64 {
        self.used_memory.load(Ordering::Relaxed)
    }

    /// The latest published resident-set-size (jemalloc `stats.resident`) figure in bytes, a single
    /// relaxed load. Surfaced for completeness; the over-limit trigger uses [`Self::used_memory`]
    /// (the live-allocated analog of Redis `used_memory`), matching how Redis's `getMaxmemoryState`
    /// compares `zmalloc_used_memory()` (allocated, not RSS) against `maxmemory`.
    #[must_use]
    pub fn used_memory_rss(&self) -> u64 {
        self.used_memory_rss.load(Ordering::Relaxed)
    }
}

impl PersistenceInfo {
    /// The persistence-OFF posture (the default cache deployment): no `data_dir`, so no snapshot is
    /// ever written, `rdb_last_save_time` is 0, and the save policy is empty. INFO still renders a
    /// `# Persistence` section with these honest zeros so monitoring sees a defined shape.
    #[must_use]
    pub fn disabled() -> Self {
        PersistenceInfo {
            enabled: false,
            rdb_last_save_time: 0,
            rdb_changes_since_last_save: 0,
            // Redis reports `rdb_last_bgsave_status:ok` even with persistence off / before any save.
            last_bgsave_ok: true,
            save_interval_secs: 0,
            save_min_changes: 0,
        }
    }
}

impl Default for PersistenceInfo {
    fn default() -> Self {
        Self::disabled()
    }
}

/// One database's line in the INFO `# Keyspace` section (durability/operability fix #5):
/// `dbN:keys=<keys>,expires=<expires>,avg_ttl=0`, the Redis shape dashboards parse. Only DBs with
/// at least one key are emitted (Redis omits empty DBs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyspaceDbLine {
    /// The logical database index (the `N` in `dbN:`).
    pub db: u32,
    /// The number of keys in the database (DBSIZE).
    pub keys: u64,
    /// The number of keys with an expiry set. NOTE: per-db expiry counting is not tracked cheaply
    /// today (it would be an O(n) scan), so the caller passes `0`; the `keys` field is the
    /// load-bearing one operators monitor, and the `expires` field is present for shape parity. A
    /// real per-db expires tally is a documented follow-up.
    pub expires: u64,
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn build_info<C: Clock>(
    clock: &C,
    server: &ServerInfo,
    rolled: CounterSnapshot,
    memory: MemoryInfo,
    effective: EffectiveMemoryConfig<'_>,
    replication: &ReplicationInfo,
    persistence: &PersistenceInfo,
    keyspace: &[KeyspaceDbLine],
    runtime_stats: RuntimeStats,
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
        // `redis_mode` honesty (#527): report `cluster` when this node booted in a clustered mode
        // (raft governance / a static shard-owner topology -- both set `cluster_enabled`), else
        // `standalone`. Matches Redis's field semantics (a node started with `cluster-enabled yes`
        // reports `redis_mode:cluster`), which `redis_exporter` reads to label the instance.
        let _ = write!(
            out,
            "redis_mode:{}\r\n",
            if server.cluster_enabled {
                "cluster"
            } else {
                "standalone"
            }
        );
        let _ = write!(out, "os:{}\r\n", std::env::consts::OS);
        let _ = write!(out, "arch_bits:{}\r\n", usize::BITS);
        let _ = write!(out, "process_id:{}\r\n", server.pid);
        // `run_id` (#527): the REAL 40-hex per-boot run id generated at boot through the determinism
        // seam (ADR-0003), carried in `ServerInfo`. No longer the 40-zero placeholder.
        let _ = write!(out, "run_id:{}\r\n", server.run_id);
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
        // PROD-7: `maxclients` is the effective connection ceiling (read from the runtime overlay so
        // a `CONFIG SET maxclients` is reflected); `blocked_clients` is the count blocked on a
        // blocking command (0 -- no blocking commands yet). Dashboards monitor connection
        // saturation off `connected_clients` / `maxclients`.
        let _ = write!(out, "maxclients:{}\r\n", runtime_stats.maxclients);
        let _ = write!(out, "blocked_clients:{}\r\n", runtime_stats.blocked_clients);
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
    if want("persistence") {
        push_persistence_section(&mut out, persistence);
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
        // #527: the cumulative bytes READ off / WRITTEN to client sockets, summed across shards by
        // the SAME `aggregate()` rollup the counters above use (invariant to which shard homed the
        // connection). `redis_exporter` reads `redis_net_input_bytes_total` /
        // `redis_net_output_bytes_total` from exactly these two INFO fields.
        let _ = write!(
            out,
            "total_net_input_bytes:{}\r\n",
            rolled.total_net_input_bytes
        );
        let _ = write!(
            out,
            "total_net_output_bytes:{}\r\n",
            rolled.total_net_output_bytes
        );
        // PROD-7: a coarse commands-per-second estimate (the caller's rolling delta off the
        // per-shard `commands_processed`) and the count of connections refused by the `maxclients`
        // gate. Both are operability signals dashboards graph.
        let _ = write!(
            out,
            "instantaneous_ops_per_sec:{}\r\n",
            runtime_stats.instantaneous_ops_per_sec
        );
        let _ = write!(
            out,
            "rejected_connections:{}\r\n",
            runtime_stats.rejected_connections
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
    if want("cpu") {
        // The `# CPU` section (PROD-7 completeness). Redis reports `used_cpu_sys`/`used_cpu_user`
        // from getrusage; IronCache does not read the OS clock outside the Env seam (ADR-0003) and
        // has no rusage seam yet, so it emits the section with `0.0` placeholders -- the SHAPE
        // dashboards / `redis_exporter` expect, without a false figure. A real CPU accounting seam
        // is a documented follow-up.
        out.push_str("# CPU\r\n");
        out.push_str("used_cpu_sys:0.000000\r\n");
        out.push_str("used_cpu_user:0.000000\r\n");
        out.push_str("\r\n");
    }
    if want("keyspace") {
        push_keyspace_section(&mut out, keyspace);
    }
    out
}

/// Append the INFO `# Persistence` section (durability footgun fix #5) to `out`, mirroring Redis
/// `rdb_*` field names so dashboards / `redis_exporter` parse "snapshot stale" off
/// `rdb_last_save_time` and `rdb_changes_since_last_save`. IronCache persists via SNAPSHOTS only (no
/// AOF), so `aof_enabled` is always `0` and the `rdb_*` fields are the durability signal. The `save`
/// line reports the REAL active save policy (the periodic cadence), or empty when off, so an
/// operator can see whether durability is actually on. `loading:0` because the readiness gate holds
/// traffic until load-on-boot completes (we never serve mid-load).
fn push_persistence_section(out: &mut String, p: &PersistenceInfo) {
    use core::fmt::Write as _;
    out.push_str("# Persistence\r\n");
    out.push_str("loading:0\r\n");
    let _ = write!(
        out,
        "rdb_changes_since_last_save:{}\r\n",
        p.rdb_changes_since_last_save
    );
    out.push_str("rdb_bgsave_in_progress:0\r\n");
    let _ = write!(out, "rdb_last_save_time:{}\r\n", p.rdb_last_save_time);
    // `rdb_last_bgsave_status:ok|err` (#549): the canonical "last save failed" signal, sourced from
    // the persistence subsystem's last-save outcome. `ok` before any save (Redis parity).
    let _ = write!(
        out,
        "rdb_last_bgsave_status:{}\r\n",
        if p.last_bgsave_ok { "ok" } else { "err" }
    );
    out.push_str("aof_enabled:0\r\n");
    let _ = write!(out, "persistence_enabled:{}\r\n", u8::from(p.enabled));
    // The active save policy as the Redis `save` directive spelling: "<secs> <changes>" when a
    // periodic cadence is configured, or empty when the periodic save is OFF.
    let save = if p.save_interval_secs > 0 {
        format!("{} {}", p.save_interval_secs, p.save_min_changes)
    } else {
        String::new()
    };
    let _ = write!(out, "save:{save}\r\n");
    out.push_str("\r\n");
}

/// Append the INFO `# Keyspace` section (operability fix #5) to `out`: one
/// `dbN:keys=<n>,expires=<m>,avg_ttl=0` line per NON-EMPTY database (Redis omits empty DBs), the
/// shape dashboards parse. The `keys` count is the live DBSIZE the caller read per db;
/// `expires`/`avg_ttl` are 0 today (per-db expiry counting is an O(n) scan, a documented follow-up),
/// with `keys` the load-bearing field. The section header is emitted even with no databases so the
/// section is always present for a section-filtered `INFO keyspace`.
fn push_keyspace_section(out: &mut String, keyspace: &[KeyspaceDbLine]) {
    use core::fmt::Write as _;
    out.push_str("# Keyspace\r\n");
    for line in keyspace {
        if line.keys == 0 {
            continue;
        }
        let _ = write!(
            out,
            "db{}:keys={},expires={},avg_ttl=0\r\n",
            line.db, line.keys, line.expires
        );
    }
    out.push_str("\r\n");
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

    #[test]
    fn per_shard_render_labels_each_shard_in_a_distinct_namespace() {
        // Two shards with distinct values: each per-shard family carries one labeled sample per
        // shard, in the additive `ironcache_shard_*` namespace (#362).
        let per_shard = vec![
            CounterSnapshot {
                commands_processed: 10,
                keyspace_hits: 3,
                keyspace_keys: 100,
                ..CounterSnapshot::default()
            },
            CounterSnapshot {
                commands_processed: 20,
                keyspace_hits: 7,
                keyspace_keys: 200,
                ..CounterSnapshot::default()
            },
        ];
        let out = render_prometheus_shards(&per_shard);
        assert!(out.contains("# TYPE ironcache_shard_commands_processed_total counter\n"));
        assert!(out.contains("ironcache_shard_commands_processed_total{shard=\"0\"} 10\n"));
        assert!(out.contains("ironcache_shard_commands_processed_total{shard=\"1\"} 20\n"));
        assert!(out.contains("ironcache_shard_keyspace_hits_total{shard=\"0\"} 3\n"));
        assert!(out.contains("ironcache_shard_keyspace_hits_total{shard=\"1\"} 7\n"));
        // Gauges with per-shard meaning are labeled too.
        assert!(out.contains("# TYPE ironcache_shard_keyspace_keys gauge\n"));
        assert!(out.contains("ironcache_shard_keyspace_keys{shard=\"1\"} 200\n"));
        // Additive: the per-shard block does NOT re-emit the unlabeled node-rollup series (no
        // mixed-label double-count within a family).
        assert!(!out.contains("\nironcache_commands_processed_total "));
        assert!(!out.contains("ironcache_commands_processed_total{"));
    }

    #[test]
    fn per_shard_snapshots_yields_one_per_registered_shard() {
        let reg = MetricsRegistry::new(3);
        assert_eq!(reg.shards(), 3);
        assert_eq!(reg.per_shard_snapshots().len(), 3);
        // A single-shard registry still yields exactly one snapshot (one `shard="0"` sample).
        assert_eq!(MetricsRegistry::new(1).per_shard_snapshots().len(), 1);
    }

    /// The find-bucket lands each observation in the SMALLEST `le` bucket whose bound is `>=` the
    /// elapsed micros (Prometheus `le` semantics), and an over-range observation lands in `+Inf`.
    #[test]
    fn latency_histogram_buckets_by_le_semantics() {
        let cell = ShardCountersCell::default();
        // Exactly on a boundary (25us) -> the `le="0.000025"` bucket (index 0).
        cell.observe_latency(25);
        // Just above a boundary (26us, > 25 and <= 50) -> the `le="0.00005"` bucket (index 1).
        cell.observe_latency(26);
        // Zero elapsed -> the smallest bucket (index 0).
        cell.observe_latency(0);
        // Above every finite bound (11s) -> the terminal `+Inf` bucket (last index).
        cell.observe_latency(11_000_000);
        let h = cell.latency_snapshot();
        assert_eq!(h.buckets[0], 2, "25us and 0us land in le=0.000025");
        assert_eq!(h.buckets[1], 1, "26us lands in le=0.00005");
        assert_eq!(h.buckets[LATENCY_BUCKET_COUNT - 1], 1, "11s lands in +Inf");
        assert_eq!(h.count, 4);
        // Sum includes the zero-elapsed observation (25 + 26 + 0 + 11s of micros).
        assert_eq!(h.sum_micros, 25 + 26 + 11_000_000);
    }

    /// The cumulative `le` series is monotonic non-decreasing, its terminal (`+Inf`) entry equals
    /// `_count`, and `_count` equals the number of observations recorded (#546 core invariant).
    #[test]
    fn latency_histogram_cumulative_is_monotonic_and_inf_equals_count() {
        let cell = ShardCountersCell::default();
        // A spread of latencies across several buckets.
        for us in [
            10u64, 40, 300, 1_500, 30_000, 700_000, 3_000_000, 20_000_000,
        ] {
            cell.observe_latency(us);
        }
        let h = cell.latency_snapshot();
        let cum = h.cumulative();
        // Monotonic non-decreasing.
        for w in cum.windows(2) {
            assert!(
                w[1] >= w[0],
                "cumulative buckets must not decrease: {cum:?}"
            );
        }
        // The `+Inf` bucket equals `_count`, which equals the 8 observations.
        assert_eq!(cum[LATENCY_BUCKET_COUNT - 1], h.count);
        assert_eq!(h.count, 8);
    }

    /// The node-wide aggregate equals the element-wise SUM of the per-shard histograms, and `_count`
    /// equals the total commands driven (K) across all shards (#546 acceptance).
    #[test]
    fn latency_histogram_node_aggregate_equals_sum_of_per_shard() {
        let reg = MetricsRegistry::new(3);
        // Drive K observations spread across the three shard cells.
        let mut k = 0u64;
        for (shard, &n) in [5usize, 8, 2].iter().enumerate() {
            let cell = reg.shard_cell(shard);
            for i in 0..n {
                // Vary the latency so several buckets are populated per shard.
                cell.observe_latency((i as u64 + 1) * 37);
                k += 1;
            }
        }
        let node = reg.aggregate_latency();
        // _count == K (total commands driven).
        assert_eq!(node.count, k);
        // The node rollup is the element-wise sum of the per-shard histograms.
        let per_shard = reg.per_shard_latency();
        let mut summed = LatencyHistogram::default();
        for h in &per_shard {
            summed = summed.merge(h);
        }
        assert_eq!(
            node, summed,
            "node aggregate must equal the sum of per-shard"
        );
        // The `+Inf` cumulative bucket equals _count on the node rollup too.
        assert_eq!(node.cumulative()[LATENCY_BUCKET_COUNT - 1], node.count);
    }

    /// The node histogram render is a valid Prometheus `histogram` family: HELP/TYPE, cumulative
    /// `le` buckets, a `+Inf` bucket equal to `_count`, plus `_sum`/`_count`.
    #[test]
    fn latency_histogram_render_is_valid_prometheus() {
        let mut h = LatencyHistogram::default();
        // 900us (le=0.001) and 2ms (le=0.0025) and 3s (le=5): three distinct buckets.
        h.buckets[LATENCY_BUCKET_BOUNDS_US
            .iter()
            .filter(|&&b| 900 > b)
            .count()] += 1;
        h.buckets[LATENCY_BUCKET_BOUNDS_US
            .iter()
            .filter(|&&b| 2_000 > b)
            .count()] += 1;
        h.buckets[LATENCY_BUCKET_BOUNDS_US
            .iter()
            .filter(|&&b| 3_000_000 > b)
            .count()] += 1;
        h.sum_micros = 900 + 2_000 + 3_000_000;
        h.count = 3;
        let out = render_latency_histogram(&h);
        assert!(
            out.contains("# TYPE ironcache_command_duration_seconds histogram\n"),
            "{out}"
        );
        // Cumulative: le=0.001 covers the 900us observation (1), le=0.0025 also covers the 2ms (2).
        assert!(
            out.contains("ironcache_command_duration_seconds_bucket{le=\"0.001\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("ironcache_command_duration_seconds_bucket{le=\"0.0025\"} 2\n"),
            "{out}"
        );
        // The `+Inf` bucket equals _count (all 3 observations).
        assert!(
            out.contains("ironcache_command_duration_seconds_bucket{le=\"+Inf\"} 3\n"),
            "{out}"
        );
        assert!(
            out.contains("ironcache_command_duration_seconds_count 3\n"),
            "{out}"
        );
        // _sum in seconds (900us + 2ms + 3s = 3.0029s).
        assert!(
            out.contains("ironcache_command_duration_seconds_sum 3.002900\n"),
            "{out}"
        );
    }

    /// The per-shard histogram render labels each shard in the distinct `ironcache_shard_*`
    /// namespace, additive to the node rollup (#362 style).
    #[test]
    fn latency_histogram_per_shard_render_labels_each_shard() {
        let per_shard = vec![
            {
                let mut h = LatencyHistogram::default();
                h.buckets[0] = 4;
                h.sum_micros = 40;
                h.count = 4;
                h
            },
            {
                let mut h = LatencyHistogram::default();
                h.buckets[LATENCY_BUCKET_COUNT - 1] = 1;
                h.sum_micros = 99_000_000;
                h.count = 1;
                h
            },
        ];
        let out = render_latency_histogram_shards(&per_shard);
        assert!(
            out.contains("# TYPE ironcache_shard_command_duration_seconds histogram\n"),
            "{out}"
        );
        assert!(
            out.contains(
                "ironcache_shard_command_duration_seconds_bucket{shard=\"0\",le=\"0.000025\"} 4\n"
            ),
            "{out}"
        );
        assert!(
            out.contains("ironcache_shard_command_duration_seconds_count{shard=\"0\"} 4\n"),
            "{out}"
        );
        assert!(
            out.contains(
                "ironcache_shard_command_duration_seconds_bucket{shard=\"1\",le=\"+Inf\"} 1\n"
            ),
            "{out}"
        );
    }

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
            // A fixed, non-zero 40-hex run id for the render tests (the real one is drawn at boot
            // from the env seam; here we only assert the field renders through).
            run_id: "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4",
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

    /// A minimal standalone [`MetricsGauges`] for the `/metrics` render tests (no raft, no repl lag).
    fn gauges() -> MetricsGauges {
        MetricsGauges {
            uptime_secs: 1,
            shards: 2,
            used_memory: 0,
            used_memory_rss: 0,
            maxmemory: 0,
            last_save_unix: 0,
            rdb_changes_since_save: 0,
            repl_link_up: true,
            repl_lag_offset: 0,
            raft: None,
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
            eff(),
            &repl(),
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
            None,
        );
        assert!(body.contains("# Server\r\n"));
        assert!(body.contains("# Clients\r\n"));
        assert!(body.contains("# Memory\r\n"));
        assert!(body.contains("# Persistence\r\n"));
        assert!(body.contains("# Stats\r\n"));
        // The `# Cluster` section reports cluster_enabled:0 in the cluster-disabled
        // default (CLUSTER_CONTRACT.md #70).
        assert!(body.contains("# Cluster\r\n"));
        assert!(body.contains("cluster_enabled:0\r\n"));
        assert!(body.contains("tcp_port:6379\r\n"));
        assert!(body.contains("connected_clients:0\r\n"));
        assert!(body.contains("mem_allocator:jemalloc\r\n"));
        assert!(body.contains(&format!("ironcache_version:{SERVER_VERSION}\r\n")));
        // #527: a non-cluster node reports `redis_mode:standalone`.
        assert!(body.contains("redis_mode:standalone\r\n"), "{body}");
        // #527: the `run_id` renders the real per-boot id from `ServerInfo`, NOT the old 40-zero
        // placeholder. It is a 40-char lowercase-hex string.
        let run_id = body
            .lines()
            .find_map(|l| l.strip_prefix("run_id:"))
            .expect("INFO must carry a run_id line");
        assert_eq!(run_id.len(), 40, "run_id must be 40 hex chars: {run_id:?}");
        assert!(
            run_id.bytes().all(|c| c.is_ascii_hexdigit()),
            "run_id must be hex: {run_id:?}"
        );
        assert_ne!(
            run_id, "0000000000000000000000000000000000000000",
            "run_id must not be the old zero placeholder"
        );
    }

    /// #527: `redis_mode` is HONEST -- a node booted in a clustered mode reports `cluster`, a
    /// standalone node reports `standalone`. Matches Redis's field semantics (`redis_exporter`
    /// reads it to label the instance).
    #[test]
    fn info_redis_mode_reflects_cluster_enabled() {
        let env = TestEnv::new(1);
        let mut clustered = server();
        clustered.cluster_enabled = true;
        let body = build_info(
            &env,
            &clustered,
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo::default(),
            &[],
            RuntimeStats::default(),
            Some("server"),
        );
        assert!(body.contains("redis_mode:cluster\r\n"), "{body}");
        assert!(!body.contains("redis_mode:standalone\r\n"), "{body}");

        // The standalone default reports `standalone` (and never `cluster`).
        let standalone = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo::default(),
            &[],
            RuntimeStats::default(),
            Some("server"),
        );
        assert!(
            standalone.contains("redis_mode:standalone\r\n"),
            "{standalone}"
        );
        assert!(
            !standalone.contains("redis_mode:cluster\r\n"),
            "{standalone}"
        );
    }

    /// Durability footgun fix #5: INFO renders a `# Persistence` section with the Redis `rdb_*`
    /// field names a dashboard parses, reflecting the live last-save time + dirty counter + save
    /// policy when persistence is ENABLED, and the honest disabled posture otherwise.
    #[test]
    fn info_persistence_section_reports_rdb_fields_and_policy() {
        let env = TestEnv::new(1);
        // ENABLED: a loaded snapshot at t=1700000000 with 7 dirty writes and a 900s/1-change policy.
        let persistence = PersistenceInfo {
            enabled: true,
            rdb_last_save_time: 1_700_000_000,
            rdb_changes_since_last_save: 7,
            last_bgsave_ok: true,
            save_interval_secs: 900,
            save_min_changes: 1,
        };
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &persistence,
            &[],
            RuntimeStats::default(),
            Some("persistence"),
        );
        assert!(body.contains("# Persistence\r\n"), "{body}");
        assert!(body.contains("loading:0\r\n"), "{body}");
        assert!(body.contains("rdb_last_save_time:1700000000\r\n"), "{body}");
        assert!(body.contains("rdb_changes_since_last_save:7\r\n"), "{body}");
        assert!(body.contains("save:900 1\r\n"), "{body}");
        assert!(body.contains("persistence_enabled:1\r\n"), "{body}");
        assert!(body.contains("aof_enabled:0\r\n"), "{body}");
        // #549: a committed-save posture renders rdb_last_bgsave_status:ok.
        assert!(body.contains("rdb_last_bgsave_status:ok\r\n"), "{body}");
        // A FAILED save renders rdb_last_bgsave_status:err (the canonical "last save failed" alert).
        let failed = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo {
                last_bgsave_ok: false,
                ..persistence
            },
            &[],
            RuntimeStats::default(),
            Some("persistence"),
        );
        assert!(
            failed.contains("rdb_last_bgsave_status:err\r\n"),
            "{failed}"
        );
        // DISABLED: the honest no-snapshot posture (last-save 0, empty policy).
        let off = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
            Some("persistence"),
        );
        assert!(off.contains("rdb_last_save_time:0\r\n"), "{off}");
        assert!(off.contains("save:\r\n"), "{off}");
        assert!(off.contains("persistence_enabled:0\r\n"), "{off}");
    }

    /// #549: the ops/sec sampler reports 0 until a second sample lands, then tracks the driven rate
    /// over the sampling window. Time is fed explicitly (the caller reads it from the Env clock).
    #[test]
    fn ops_per_sec_sampler_tracks_the_driven_rate() {
        let s = OpsPerSecSampler::new();
        // The first sample seeds the ring: no elapsed time yet, so the rate is 0.
        assert_eq!(s.observe(0, 0), 0, "first sample -> no rate yet");
        // 500 commands over the next 1000ms -> 500 ops/sec.
        assert_eq!(s.observe(1000, 500), 500, "500 commands / 1s = 500 ops/sec");
        // Sustained: another 1000 commands over the next 2000ms (t=3000) -> the window (t=0..3000,
        // 1500 commands) averages 500 ops/sec.
        assert_eq!(s.observe(3000, 1500), 500, "sustained rate over the window");
    }

    /// #549: the sampler is robust to a non-advancing / backwards wall clock (a benign no-op for this
    /// cold metric): a same-millisecond read coalesces into the newest sample (no zero-duration
    /// division) and never panics.
    #[test]
    fn ops_per_sec_sampler_is_robust_to_a_stalled_clock() {
        let s = OpsPerSecSampler::new();
        assert_eq!(s.observe(1000, 0), 0);
        // Same instant, more commands: coalesced, still no elapsed time -> 0 (no panic).
        assert_eq!(s.observe(1000, 100), 0);
        // A backwards clock read is also coalesced rather than producing a negative window.
        assert_eq!(s.observe(500, 200), 0);
        // Time finally advances: the rate is computed against the coalesced newest total (200) at the
        // retained oldest sample.
        assert_eq!(s.observe(2000, 1200), 1000);
    }

    /// Operability fix #5: INFO renders a `# Keyspace` section with `dbN:keys=...` lines (Redis
    /// shape) for non-empty databases, and omits empty ones.
    #[test]
    fn info_keyspace_section_reports_db_key_counts() {
        let env = TestEnv::new(1);
        let keyspace = [
            KeyspaceDbLine {
                db: 0,
                keys: 42,
                expires: 0,
            },
            KeyspaceDbLine {
                db: 3,
                keys: 5,
                expires: 0,
            },
        ];
        let body = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo::disabled(),
            &keyspace,
            RuntimeStats::default(),
            Some("keyspace"),
        );
        assert!(body.contains("# Keyspace\r\n"), "{body}");
        assert!(
            body.contains("db0:keys=42,expires=0,avg_ttl=0\r\n"),
            "{body}"
        );
        assert!(
            body.contains("db3:keys=5,expires=0,avg_ttl=0\r\n"),
            "{body}"
        );
        // An empty database is omitted (no db1/db2 line).
        assert!(!body.contains("db1:"), "{body}");
        assert!(!body.contains("db2:"), "{body}");
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
            Some("server"),
        );
        assert!(only_server.contains("# Server\r\n"));
        assert!(!only_server.contains("# Memory\r\n"));
        // The `# Cluster` section is gated by the filter too: a server-only INFO omits it.
        assert!(!only_server.contains("# Cluster\r\n"));
        // The new `# Persistence` / `# Keyspace` sections are gated by the filter too (fix #5).
        assert!(!only_server.contains("# Persistence\r\n"));
        assert!(!only_server.contains("# Keyspace\r\n"));
        // Asking for the cluster section yields it with the disabled flag.
        let only_cluster = build_info(
            &env,
            &server(),
            CounterSnapshot::default(),
            MemoryInfo::default(),
            eff(),
            &repl(),
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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
            &PersistenceInfo::disabled(),
            &[],
            RuntimeStats::default(),
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

    /// #556: the coordinator hop counters record per shard and SUM in the node rollup, and a
    /// `CONFIG RESETSTAT` (`apply(reset_stats)`) zeroes them like the other `_total` stats.
    #[test]
    fn hop_counters_record_merge_and_reset() {
        let mut a = ShardCounters::new();
        // Shard a: paid 3 hops, served 1 for a peer, served 4 keyed requests locally.
        for _ in 0..3 {
            a.on_hop_sent();
        }
        a.on_hop_served();
        for _ in 0..4 {
            a.on_local_served();
        }
        let b = ShardCounters::new();
        // Shard b: served 2 peer hops, 5 locally, sent none.
        for _ in 0..2 {
            b.on_hop_served();
        }
        for _ in 0..5 {
            b.on_local_served();
        }
        let rolled = a.snapshot().merge(b.snapshot());
        assert_eq!(rolled.hops_sent, 3, "3 + 0");
        assert_eq!(rolled.hops_served, 3, "1 + 2");
        assert_eq!(rolled.local_served, 9, "4 + 5");
        // hop-rate = hops_sent / (hops_sent + local_served) = 3 / 12 = 0.25, checked as integers
        // (denominator 12, four times the numerator) to avoid a float compare.
        assert_eq!(rolled.hops_sent + rolled.local_served, 12);
        assert_eq!(rolled.hops_sent + rolled.local_served, rolled.hops_sent * 4);
        // RESETSTAT zeroes the hop `_total`s (they are since-start stats like commands_processed).
        a.apply(CounterDeltas {
            reset_stats: true,
            ..CounterDeltas::default()
        });
        let after = a.snapshot();
        assert_eq!(after.hops_sent, 0);
        assert_eq!(after.hops_served, 0);
        assert_eq!(after.local_served, 0);
    }

    /// #556: the hop counters render in BOTH the node rollup (`ironcache_*`) and the per-shard
    /// (`ironcache_shard_*{shard}`) families, mirroring the other counter families.
    #[test]
    fn prometheus_render_emits_hop_counter_families() {
        let counters = CounterSnapshot {
            hops_sent: 7,
            hops_served: 4,
            local_served: 21,
            ..CounterSnapshot::default()
        };
        let node = render_prometheus(counters, gauges());
        assert!(
            node.contains(
                "# TYPE ironcache_hops_sent_total counter\nironcache_hops_sent_total 7\n"
            ),
            "{node}"
        );
        assert!(node.contains("ironcache_hops_served_total 4\n"), "{node}");
        assert!(node.contains("ironcache_local_served_total 21\n"), "{node}");

        let s0 = CounterSnapshot {
            hops_sent: 7,
            local_served: 1,
            ..CounterSnapshot::default()
        };
        let s1 = CounterSnapshot {
            hops_served: 4,
            local_served: 20,
            ..CounterSnapshot::default()
        };
        let per = render_prometheus_shards(&[s0, s1]);
        assert!(
            per.contains("ironcache_shard_hops_sent_total{shard=\"0\"} 7\n"),
            "{per}"
        );
        assert!(
            per.contains("ironcache_shard_hops_served_total{shard=\"1\"} 4\n"),
            "{per}"
        );
        assert!(
            per.contains("ironcache_shard_local_served_total{shard=\"1\"} 20\n"),
            "{per}"
        );
    }

    /// #556: the inbox-depth gauge renders a node-wide sum plus a per-shard sample.
    #[test]
    fn inbox_depth_renders_node_and_per_shard() {
        let body = render_inbox_depth(&[2, 0, 5]);
        assert!(
            body.contains("# TYPE ironcache_inbox_depth gauge\nironcache_inbox_depth 7\n"),
            "node-wide sum 2+0+5: {body}"
        );
        assert!(
            body.contains("ironcache_shard_inbox_depth{shard=\"0\"} 2\n"),
            "{body}"
        );
        assert!(
            body.contains("ironcache_shard_inbox_depth{shard=\"2\"} 5\n"),
            "{body}"
        );
    }

    /// The registry pre-allocates one cell per shard, a shard adopts its cell by index, and
    /// `aggregate` sums every shard's live cell into the node-wide rollup (the cross-thread
    /// read the `/metrics` endpoint performs). After N commands across two shards the processed
    /// counter shows N.
    #[test]
    fn registry_aggregates_across_shards() {
        let reg = MetricsRegistry::new(2);
        assert_eq!(reg.shards(), 2);
        // Two shards, each wrapping its registry cell, mutate independently.
        let mut s0 = ShardCounters::with_cell(reg.shard_cell(0));
        let mut s1 = ShardCounters::with_cell(reg.shard_cell(1));
        s0.on_connection_open();
        s0.on_command();
        s0.on_command(); // shard 0: 2 commands
        s1.on_connection_open();
        s1.on_connection_open();
        s1.on_command(); // shard 1: 1 command, 2 conns
        // The registry reads the SAME cells the shards mutated (cross-cell aggregation).
        let rolled = reg.aggregate();
        assert_eq!(rolled.commands_processed, 3, "2 + 1 across the two shards");
        assert_eq!(
            rolled.connections_received, 3,
            "1 + 2 across the two shards"
        );
        assert_eq!(rolled.connected_clients, 3);
        // The keyspace gauge is published off-band; sums across shards.
        reg.shard_cell(0).set_keyspace_keys(10);
        reg.shard_cell(1).set_keyspace_keys(7);
        assert_eq!(reg.aggregate().keyspace_keys, 17);
    }

    /// `apply(reset_stats)` zeroes the resettable totals on the shard's cell (CONFIG RESETSTAT),
    /// leaving the live `connected_clients` gauge alone, and the registry reads the reset values.
    #[test]
    fn registry_reflects_resetstat() {
        let reg = MetricsRegistry::new(1);
        let mut s = ShardCounters::with_cell(reg.shard_cell(0));
        s.on_connection_open();
        s.on_command();
        s.on_evicted(5);
        s.apply(CounterDeltas {
            reset_stats: true,
            ..Default::default()
        });
        let rolled = reg.aggregate();
        assert_eq!(rolled.commands_processed, 0);
        assert_eq!(rolled.evicted_keys, 0);
        assert_eq!(
            rolled.connected_clients, 1,
            "the live gauge survives RESETSTAT"
        );
    }

    /// #661: `block_guard` is a leak-free per-shard `blocked_clients` gauge -- increments on
    /// creation, decrements on Drop (every park exit), summed node-wide by the registry. A client
    /// parks on exactly one shard, so the node-wide figure is the sum of the per-shard gauges.
    #[test]
    fn block_guard_tracks_blocked_clients_node_wide_and_leaks_none() {
        let reg = MetricsRegistry::new(2);
        let s0 = ShardCounters::with_cell(reg.shard_cell(0));
        let s1 = ShardCounters::with_cell(reg.shard_cell(1));
        assert_eq!(reg.aggregate().blocked_clients, 0, "no parks yet");

        let g0 = s0.block_guard();
        let g1 = s1.block_guard();
        assert_eq!(
            reg.aggregate().blocked_clients,
            2,
            "two parked clients across two shards"
        );
        drop(g0);
        assert_eq!(reg.aggregate().blocked_clients, 1, "one park exited");
        drop(g1);
        assert_eq!(
            reg.aggregate().blocked_clients,
            0,
            "all parks exited -- the gauge leaks nothing"
        );

        // Nested parks on the SAME shard count as a gauge (not a boolean), and unwind cleanly.
        let a = s0.block_guard();
        let b = s0.block_guard();
        assert_eq!(reg.aggregate().blocked_clients, 2, "two parks on one shard");
        drop(a);
        drop(b);
        assert_eq!(reg.aggregate().blocked_clients, 0);
    }

    /// The Prometheus renderer emits valid `# HELP`/`# TYPE` + `name value` lines for the
    /// aggregated counters and gauges, and OMITS the raft families when not in raft mode.
    #[test]
    fn prometheus_render_standalone_has_no_raft() {
        let counters = CounterSnapshot {
            commands_processed: 42,
            connected_clients: 3,
            keyspace_keys: 100,
            ..Default::default()
        };
        let gauges = MetricsGauges {
            uptime_secs: 7,
            shards: 4,
            used_memory: 1024,
            used_memory_rss: 2048,
            maxmemory: 0,
            last_save_unix: 0,
            rdb_changes_since_save: 0,
            repl_link_up: true,
            repl_lag_offset: 0,
            raft: None,
        };
        let body = render_prometheus(counters, gauges);
        assert!(body.contains("# TYPE ironcache_commands_processed_total counter\n"));
        // #549: the replication gauges appear in the scrape (a standalone master: link up, no lag).
        assert!(
            body.contains("# TYPE ironcache_replication_link_up gauge\n"),
            "{body}"
        );
        assert!(body.contains("ironcache_replication_link_up 1\n"), "{body}");
        assert!(
            body.contains("ironcache_replication_lag_offset 0\n"),
            "{body}"
        );
        assert!(
            body.contains("ironcache_commands_processed_total 42\n"),
            "{body}"
        );
        assert!(body.contains("# TYPE ironcache_connected_clients gauge\n"));
        assert!(body.contains("ironcache_connected_clients 3\n"), "{body}");
        assert!(body.contains("ironcache_keyspace_keys 100\n"), "{body}");
        assert!(body.contains("ironcache_uptime_seconds 7\n"), "{body}");
        assert!(
            body.contains("ironcache_used_memory_bytes 1024\n"),
            "{body}"
        );
        // No raft series on a standalone node.
        assert!(!body.contains("ironcache_raft_"), "{body}");
    }

    /// In raft mode the renderer adds the `ironcache_raft_*` gauges.
    #[test]
    fn prometheus_render_raft_emits_raft_series() {
        let gauges = MetricsGauges {
            uptime_secs: 1,
            shards: 1,
            used_memory: 0,
            used_memory_rss: 0,
            maxmemory: 0,
            last_save_unix: 0,
            rdb_changes_since_save: 0,
            repl_link_up: true,
            repl_lag_offset: 0,
            raft: Some(RaftGauges {
                is_leader: true,
                current_term: 9,
                commit_index: 17,
                voters: 3,
            }),
        };
        let body = render_prometheus(CounterSnapshot::default(), gauges);
        assert!(body.contains("ironcache_raft_is_leader 1\n"), "{body}");
        assert!(body.contains("ironcache_raft_current_term 9\n"), "{body}");
        assert!(body.contains("ironcache_raft_commit_index 17\n"), "{body}");
        assert!(body.contains("ironcache_raft_voters 3\n"), "{body}");
    }

    /// PROD-SAFETY #3: the connection gate admits up to `maxclients`, REJECTS the connection over
    /// the cap WITHOUT counting it, and a `release` frees a slot for the next connection.
    #[test]
    fn connection_gate_enforces_maxclients() {
        let gate = ConnectionGate::new();
        // Cap of 3: the first three admit, the fourth is rejected without bumping the count.
        assert!(gate.try_admit(3));
        assert!(gate.try_admit(3));
        assert!(gate.try_admit(3));
        assert_eq!(gate.live(), 3);
        assert!(
            !gate.try_admit(3),
            "the 4th connection over the cap must be rejected"
        );
        // A rejected connection was NOT counted: the live count stays at the cap.
        assert_eq!(gate.live(), 3);
        // Releasing a slot lets the next connection in.
        gate.release();
        assert_eq!(gate.live(), 2);
        assert!(gate.try_admit(3));
        assert_eq!(gate.live(), 3);
    }

    /// PROD-SAFETY #3: `maxclients == 0` DISABLES the cap (unlimited connections, the pre-fix
    /// behavior), while still tracking the live count so a later `CONFIG SET maxclients` enforces
    /// against a true figure.
    #[test]
    fn connection_gate_zero_cap_is_unlimited() {
        let gate = ConnectionGate::new();
        for _ in 0..10_000 {
            assert!(gate.try_admit(0), "a 0 cap never rejects");
        }
        assert_eq!(gate.live(), 10_000);
        // release saturates at 0 (never underflows).
        for _ in 0..10_001 {
            gate.release();
        }
        assert_eq!(gate.live(), 0);
    }

    /// PROD-SAFETY #1/#2: the process-memory gauge publishes/reads the allocator figures, and a
    /// fresh (un-published) gauge reads 0 so the admission gate falls back to the logical counter.
    #[test]
    fn process_memory_gauge_publishes_and_reads() {
        let gauge = ProcessMemoryGauge::new();
        // Fresh: 0 means "no allocator figure available" -> the gate falls back to the logical path.
        assert_eq!(gauge.used_memory(), 0);
        assert_eq!(gauge.used_memory_rss(), 0);
        gauge.publish(4_000, 8_192);
        assert_eq!(gauge.used_memory(), 4_000);
        assert_eq!(gauge.used_memory_rss(), 8_192);
        // A later publish overwrites (last writer wins; eventually-consistent by design).
        gauge.publish(1_234, 5_678);
        assert_eq!(gauge.used_memory(), 1_234);
        assert_eq!(gauge.used_memory_rss(), 5_678);
    }
}
