// SPDX-License-Identifier: MIT OR Apache-2.0
//! Operator-introspection state for the admin command family (PROD-7): the SLOWLOG ring,
//! the LATENCY monitor, and the live-connection registry that CLIENT KILL / CLIENT PAUSE
//! act through.
//!
//! ## Why these live behind ONE small lock each (the shared-nothing carve-out)
//!
//! The hot path is shared-nothing (ADR-0002): each shard owns its per-key state with no
//! lock. These three structures are DELIBERATELY shared, NODE-LEVEL, and OFF the per-command
//! fast path, so they use a single standard-library mutex each with the sanctioned
//! shared-nothing lint-allow marker:
//!
//! - [`SlowLog`]: the ring is appended to ONLY when a command was actually slow (elapsed >=
//!   the configured threshold), which is rare by construction; when SLOWLOG is DISABLED
//!   (`slowlog-log-slower-than` = -1) the caller never even reads the clock and never touches
//!   the lock (a single atomic compare gates the whole hook). SLOWLOG GET/LEN/RESET are admin
//!   commands (rare). A shared ring gives one coherent, ordered, monotonically-ID'd log
//!   without the cross-shard fan-out a per-shard ring would need on GET (justified per the
//!   PROD-7 brief). The lock is NEVER taken on a fast command.
//! - [`LatencyMonitor`]: updated on the same slow-path as the SLOWLOG (only when a command
//!   exceeds a small fixed sampling floor), and queried only by the LATENCY admin command.
//! - [`ClientRegistry`]: mutated only on connection OPEN / CLOSE (cold accept/teardown, not
//!   per command) and on the rare CLIENT KILL / CLIENT LIST / CLIENT PAUSE admin command.
//!
//! None of these is touched per stored key, so `bytes_per_key` and the steady-state command
//! throughput are unaffected.

use std::collections::BTreeMap;
use std::sync::Mutex; // lint-allow: shared-nothing -- the sanctioned node-level admin lock, off the per-command hot path (see module docs).
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// The DEFAULT `slowlog-log-slower-than` threshold in MICROSECONDS (Redis default 10000us =
/// 10ms). `-1` disables the SLOWLOG entirely; `0` logs every command.
pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN: i64 = 10_000;

/// The DEFAULT `slowlog-max-len` (Redis default 128 entries).
pub const DEFAULT_SLOWLOG_MAX_LEN: u64 = 128;

/// The fixed floor (microseconds) below which a command is NEVER sampled into the LATENCY
/// monitor's `command` event, so the monitor only ever records meaningful spikes and the
/// slow-path bookkeeping stays bounded. Mirrors Redis's `LATENCY_THRESHOLD` defaulting to a
/// non-trivial value; here a 1ms floor for the always-on `command` event.
pub const LATENCY_COMMAND_FLOOR_MICROS: u64 = 1_000;

/// One recorded SLOWLOG entry (SLOWLOG GET reply shape, Redis `slowlogEntry`): a unique id, the
/// unix time the command ran, the microseconds it took, the command + args, and the client
/// address + name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlowLogEntry {
    /// The monotonic per-process entry id (newest has the highest id). SLOWLOG GET returns
    /// newest-first.
    pub id: u64,
    /// The unix TIMESTAMP (seconds) at which the command was logged.
    pub unix_time_secs: u64,
    /// The execution time in MICROSECONDS.
    pub micros: u64,
    /// The command and its arguments, each as the raw bytes seen on the wire. Redis caps both
    /// the number of args kept (31, with a synthetic "... N more arguments" tail) and the length
    /// of each arg (128 bytes, truncated with a "... N more bytes" tail); we apply the same caps
    /// so a pathological command cannot bloat the ring.
    pub args: Vec<Vec<u8>>,
    /// The client peer address (`ip:port`) that issued the command.
    pub client_addr: String,
    /// The client name (CLIENT SETNAME), empty if unset.
    pub client_name: String,
}

/// Redis caps: at most this many ARGS are retained per SLOWLOG entry; beyond it a synthetic
/// trailing arg `... <n> more arguments` is appended (matching `slowlogCreateEntry`).
const SLOWLOG_ENTRY_MAX_ARGS: usize = 31;
/// Redis caps: each retained arg is truncated to this many BYTES; beyond it the arg becomes a
/// prefix plus `... (<n> more bytes)`.
const SLOWLOG_ENTRY_MAX_ARG_LEN: usize = 128;

impl SlowLogEntry {
    /// Build an entry, applying the Redis arg-count and per-arg-length caps so a single huge
    /// command cannot bloat the shared ring. PURE (no clock); the caller supplies `id`, the unix
    /// time, and the elapsed micros it measured through the Env seam.
    #[must_use]
    pub fn capped(
        id: u64,
        unix_time_secs: u64,
        micros: u64,
        raw_args: &[Vec<u8>],
        client_addr: String,
        client_name: String,
    ) -> SlowLogEntry {
        let mut args: Vec<Vec<u8>> = Vec::new();
        let total = raw_args.len();
        let keep = total.min(SLOWLOG_ENTRY_MAX_ARGS);
        for a in &raw_args[..keep] {
            if a.len() > SLOWLOG_ENTRY_MAX_ARG_LEN {
                let mut t = a[..SLOWLOG_ENTRY_MAX_ARG_LEN].to_vec();
                let extra = a.len() - SLOWLOG_ENTRY_MAX_ARG_LEN;
                t.extend_from_slice(format!("... ({extra} more bytes)").as_bytes());
                args.push(t);
            } else {
                args.push(a.clone());
            }
        }
        if total > SLOWLOG_ENTRY_MAX_ARGS {
            let more = total - SLOWLOG_ENTRY_MAX_ARGS;
            args.push(format!("... ({more} more arguments)").into_bytes());
        }
        SlowLogEntry {
            id,
            unix_time_secs,
            micros,
            args,
            client_addr,
            client_name,
        }
    }
}

/// The node-level SLOWLOG: a bounded ring of the slowest recent commands, plus the live
/// `slowlog-log-slower-than` / `slowlog-max-len` knobs. ONE per node, shared by `Arc` onto
/// every shard's [`crate`] context.
///
/// The two knobs are lock-free atomics so the per-command HOOK reads the threshold with a
/// single relaxed load and, when it is `-1` (disabled), short-circuits BEFORE reading the
/// clock or touching the entry lock. The ring itself is behind one `Mutex` taken ONLY when a
/// command is actually slow (rare) or by a SLOWLOG admin command (rare); see the module docs
/// for the shared-nothing justification.
#[derive(Debug)]
pub struct SlowLog {
    /// `slowlog-log-slower-than` in microseconds. `-1` disables; `0` logs everything. Relaxed
    /// atomic so the per-command hook reads it with no lock.
    log_slower_than_micros: AtomicI64,
    /// `slowlog-max-len`: the maximum number of retained entries (the ring drops the OLDEST
    /// when full). Relaxed atomic.
    max_len: AtomicU64,
    /// The next entry id to assign (monotonic). Relaxed atomic; bumped under the entry lock when
    /// an entry is pushed.
    next_id: AtomicU64,
    /// The retained entries, newest at the FRONT (index 0). Behind the one justified lock.
    entries: Mutex<std::collections::VecDeque<SlowLogEntry>>, // lint-allow: shared-nothing -- see module docs.
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new()
    }
}

impl SlowLog {
    /// A fresh SLOWLOG with the Redis defaults (10ms threshold, 128-entry cap).
    #[must_use]
    pub fn new() -> SlowLog {
        SlowLog::with_config(DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_LEN)
    }

    /// A SLOWLOG seeded with explicit knobs (the boot-config values).
    #[must_use]
    pub fn with_config(log_slower_than_micros: i64, max_len: u64) -> SlowLog {
        SlowLog {
            log_slower_than_micros: AtomicI64::new(log_slower_than_micros),
            max_len: AtomicU64::new(max_len),
            next_id: AtomicU64::new(0),
            entries: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// The current `slowlog-log-slower-than` threshold in microseconds (relaxed load). The
    /// per-command hook reads THIS first; `-1` means SLOWLOG is disabled and the hook does
    /// nothing further (no clock read, no lock).
    #[must_use]
    pub fn log_slower_than_micros(&self) -> i64 {
        self.log_slower_than_micros.load(Ordering::Relaxed)
    }

    /// Whether the SLOWLOG is ENABLED (threshold >= 0). The single relaxed compare the hot-path
    /// hook gates on so a disabled SLOWLOG costs at most one atomic load + branch per command.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.log_slower_than_micros() >= 0
    }

    /// `CONFIG SET slowlog-log-slower-than <micros>` (relaxed store).
    pub fn set_log_slower_than_micros(&self, v: i64) {
        self.log_slower_than_micros.store(v, Ordering::Relaxed);
    }

    /// The current `slowlog-max-len` (relaxed load).
    #[must_use]
    pub fn max_len(&self) -> u64 {
        self.max_len.load(Ordering::Relaxed)
    }

    /// `CONFIG SET slowlog-max-len <n>` (relaxed store). The ring is trimmed to the new length on
    /// the next push (or immediately by [`Self::trim_to_max`]).
    pub fn set_max_len(&self, n: u64) {
        self.max_len.store(n, Ordering::Relaxed);
        self.trim_to_max();
    }

    /// Record a slow command. The caller has ALREADY decided `micros >= threshold` (the hook
    /// gates on [`Self::enabled`] + compares against [`Self::log_slower_than_micros`] before
    /// calling), so this just assigns the next id and pushes to the FRONT, dropping the oldest
    /// past `max_len`. Takes the lock (rare). `max_len == 0` keeps NO entries (Redis clears the
    /// ring when max-len is 0).
    pub fn record(
        &self,
        unix_time_secs: u64,
        micros: u64,
        raw_args: &[Vec<u8>],
        client_addr: String,
        client_name: String,
    ) {
        let max = self.max_len();
        if max == 0 {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = SlowLogEntry::capped(
            id,
            unix_time_secs,
            micros,
            raw_args,
            client_addr,
            client_name,
        );
        let mut g = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.push_front(entry);
        while g.len() as u64 > max {
            g.pop_back();
        }
    }

    /// SLOWLOG GET [count]: the newest `count` entries (newest first). `None` returns ALL (Redis
    /// treats a missing count as the default 10; the COMMAND layer passes the resolved count).
    #[must_use]
    pub fn get(&self, count: Option<usize>) -> Vec<SlowLogEntry> {
        let g = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let n = count.unwrap_or(g.len()).min(g.len());
        g.iter().take(n).cloned().collect()
    }

    /// SLOWLOG LEN: the number of retained entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether the ring is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// SLOWLOG RESET: drop every entry. The id counter is NOT reset (Redis keeps assigning
    /// monotonically increasing ids across resets).
    pub fn reset(&self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Trim the ring to the current `max_len` (called after lowering the cap).
    fn trim_to_max(&self) {
        let max = self.max_len();
        let mut g = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while g.len() as u64 > max {
            g.pop_back();
        }
    }
}

/// One LATENCY event's worst spike (the minimal real monitor): the timestamp + magnitude of the
/// single worst latency sample observed for the event, plus a small bounded history.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LatencyEvent {
    /// Unix seconds of the WORST sample seen for this event.
    worst_unix_secs: u64,
    /// The WORST latency magnitude in MILLISECONDS (Redis LATENCY reports milliseconds).
    worst_ms: u64,
    /// A bounded recent history of `(unix_secs, ms)` samples (newest pushed to the back), for
    /// LATENCY HISTORY. Capped so the monitor stays bounded.
    history: std::collections::VecDeque<(u64, u64)>,
}

/// The per-event history cap (Redis keeps `LATENCY_TS_LEN` = 160 samples; a smaller bounded
/// window is plenty for the v1 monitor).
const LATENCY_HISTORY_LEN: usize = 160;

/// The minimal real LATENCY monitor (PROD-7): the worst spike + a bounded history per named
/// event. ONE per node, shared by `Arc`. Updated on the same slow-path the SLOWLOG hook runs on
/// (only when a sample exceeds a floor), and read only by the LATENCY admin command, so it is
/// off the per-command hot path. The `command` event is the always-tracked one (every command's
/// elapsed time is a candidate sample); a subsystem can record its own named events later.
#[derive(Debug, Default)]
pub struct LatencyMonitor {
    events: Mutex<BTreeMap<String, LatencyEvent>>, // lint-allow: shared-nothing -- node-level admin state, off the hot path.
}

impl LatencyMonitor {
    /// A fresh, empty monitor.
    #[must_use]
    pub fn new() -> LatencyMonitor {
        LatencyMonitor::default()
    }

    /// Record one latency `sample_ms` for `event` at `unix_secs`: update the worst spike if this
    /// sample is larger and push it to the bounded history. Takes the lock (rare, off the hot
    /// path; the caller already gated on a floor).
    pub fn record(&self, event: &str, unix_secs: u64, sample_ms: u64) {
        let mut g = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let e = g.entry(event.to_owned()).or_insert(LatencyEvent {
            worst_unix_secs: unix_secs,
            worst_ms: 0,
            history: std::collections::VecDeque::new(),
        });
        if sample_ms >= e.worst_ms {
            e.worst_ms = sample_ms;
            e.worst_unix_secs = unix_secs;
        }
        e.history.push_back((unix_secs, sample_ms));
        while e.history.len() > LATENCY_HISTORY_LEN {
            e.history.pop_front();
        }
    }

    /// LATENCY LATEST: `(event, worst_unix_secs, worst_ms, max_ms)` per tracked event, sorted by
    /// event name. The last two are equal in this monitor (we track a single worst spike, which
    /// is both the latest and the max), matching the Redis 4-tuple shape.
    #[must_use]
    pub fn latest(&self) -> Vec<(String, u64, u64, u64)> {
        let g = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.iter()
            .map(|(name, e)| (name.clone(), e.worst_unix_secs, e.worst_ms, e.worst_ms))
            .collect()
    }

    /// LATENCY HISTORY <event>: the bounded `(unix_secs, ms)` samples for `event`, oldest first.
    /// Empty when the event is unknown.
    #[must_use]
    pub fn history(&self, event: &str) -> Vec<(u64, u64)> {
        let g = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.get(event)
            .map(|e| e.history.iter().copied().collect())
            .unwrap_or_default()
    }

    /// LATENCY RESET [event...]: reset ALL events (when `events` is empty) or only the named ones;
    /// returns the number of events actually removed (Redis returns the count reset).
    #[must_use]
    pub fn reset(&self, events: &[String]) -> usize {
        let mut g = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if events.is_empty() {
            let n = g.len();
            g.clear();
            return n;
        }
        let mut n = 0;
        for ev in events {
            if g.remove(ev).is_some() {
                n += 1;
            }
        }
        n
    }

    /// The number of currently tracked events (for LATENCY DOCTOR's summary).
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

/// One live connection's registry record: the facts CLIENT LIST / CLIENT INFO render and CLIENT
/// KILL filters on, plus the lock-free flags KILL / PAUSE set that the connection's serve loop
/// observes.
#[derive(Debug)]
pub struct ClientHandle {
    /// The client id (CLIENT ID).
    pub id: u64,
    /// The peer address (`ip:port`).
    pub addr: String,
    /// The local (server) address.
    pub laddr: String,
    /// The client name (updated on CLIENT SETNAME).
    pub name: Mutex<String>, // lint-allow: shared-nothing -- node-level admin state, off the hot path.
    /// The selected DB (updated on SELECT). Relaxed atomic.
    pub db: AtomicU64,
    /// The KILL flag: set by CLIENT KILL; the connection's serve loop checks it after each command
    /// batch and closes itself. Lock-free.
    kill: AtomicBool,
}

impl ClientHandle {
    /// Whether this connection has been marked for KILL.
    #[must_use]
    pub fn is_killed(&self) -> bool {
        self.kill.load(Ordering::Relaxed)
    }

    /// Mark this connection for KILL (the serve loop closes it after its current batch).
    pub fn kill(&self) {
        self.kill.store(true, Ordering::Relaxed);
    }

    /// The current client name (a clone of the locked string).
    #[must_use]
    pub fn name(&self) -> String {
        self.name
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// The node-level live-connection registry: the directory CLIENT LIST / CLIENT KILL / CLIENT
/// PAUSE operate over. ONE per node, shared by `Arc`. A connection REGISTERS itself on accept and
/// DEREGISTERS on close (cold paths, not per command). CLIENT KILL flips a target's `kill` flag;
/// CLIENT PAUSE sets a node-wide pause deadline the serve loop honors.
#[derive(Debug, Default)]
pub struct ClientRegistry {
    clients: Mutex<BTreeMap<u64, std::sync::Arc<ClientHandle>>>, // lint-allow: shared-nothing -- node-level admin state, off the hot path.
    /// The CLIENT PAUSE deadline as a MONOTONIC-millis value (the Env clock's `now().as_millis`
    /// basis the serve layer supplies). `0` means not paused. Relaxed atomic; the serve loop reads
    /// it after each command batch.
    pause_until_mono_ms: AtomicU64,
    /// Whether the active pause is WRITE-only (`true`) or ALL commands (`false`). Meaningful only
    /// while `pause_until_mono_ms` is in the future.
    pause_writes_only: AtomicBool,
}

impl ClientRegistry {
    /// A fresh empty registry.
    #[must_use]
    pub fn new() -> ClientRegistry {
        ClientRegistry::default()
    }

    /// Register a new connection, returning the shared [`ClientHandle`] the connection keeps (to
    /// observe its own KILL flag + publish SETNAME / SELECT). Called once on accept.
    pub fn register(
        &self,
        id: u64,
        addr: String,
        laddr: String,
        db: u64,
    ) -> std::sync::Arc<ClientHandle> {
        let handle = std::sync::Arc::new(ClientHandle {
            id,
            addr,
            laddr,
            name: Mutex::new(String::new()),
            db: AtomicU64::new(db),
            kill: AtomicBool::new(false),
        });
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, std::sync::Arc::clone(&handle));
        handle
    }

    /// Deregister a connection (on close). Idempotent.
    pub fn deregister(&self, id: u64) {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    /// The number of live connections.
    #[must_use]
    pub fn len(&self) -> usize {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether there are no live connections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Look up a handle by id.
    #[must_use]
    pub fn by_id(&self, id: u64) -> Option<std::sync::Arc<ClientHandle>> {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// Find the handle whose peer address matches `addr` exactly.
    #[must_use]
    pub fn by_addr(&self, addr: &str) -> Option<std::sync::Arc<ClientHandle>> {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .find(|h| h.addr == addr)
            .cloned()
    }

    /// A snapshot of every live handle (for CLIENT LIST), sorted by id.
    #[must_use]
    pub fn snapshot(&self) -> Vec<std::sync::Arc<ClientHandle>> {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// CLIENT KILL ID <id>: mark the matching connection for KILL. Returns whether a connection
    /// matched.
    pub fn kill_id(&self, id: u64) -> bool {
        match self.by_id(id) {
            Some(h) => {
                h.kill();
                true
            }
            None => false,
        }
    }

    /// CLIENT KILL ADDR <addr>: mark the connection at `addr` for KILL. Returns whether one
    /// matched.
    pub fn kill_addr(&self, addr: &str) -> bool {
        match self.by_addr(addr) {
            Some(h) => {
                h.kill();
                true
            }
            None => false,
        }
    }

    /// CLIENT PAUSE <ms> [WRITE|ALL]: pause command processing until `now_mono_ms + ms`. The serve
    /// loop gates each command before dispatch: a WRITE pause holds only write commands (reads and
    /// admin like SAVE/INFO/PING proceed); an ALL pause holds every command. See
    /// [`Self::pause_write_remaining_ms`] / [`Self::pause_all_remaining_ms`].
    pub fn pause(&self, now_mono_ms: u64, ms: u64, writes_only: bool) {
        self.pause_until_mono_ms
            .store(now_mono_ms.saturating_add(ms), Ordering::Relaxed);
        self.pause_writes_only.store(writes_only, Ordering::Relaxed);
    }

    /// CLIENT UNPAUSE: clear any active pause.
    pub fn unpause(&self) {
        self.pause_until_mono_ms.store(0, Ordering::Relaxed);
    }

    /// The remaining milliseconds of the active pause window given the monotonic-millis `now`
    /// (`0` when none). This is AGNOSTIC to the pause KIND: it returns the same non-zero remaining
    /// for a WRITE-only pause as for an ALL pause. It is the raw window the kind-aware helpers
    /// ([`Self::pause_all_remaining_ms`] for the post-batch stall, [`Self::pause_write_remaining_ms`]
    /// for the per-write-command stall) build on. A WRITE-only pause now lets reads + admin proceed:
    /// the serve loop's POST-BATCH stall consults `pause_all_remaining_ms` (which is `0` for a
    /// WRITE-only pause, so reads/PING/INFO/SAVE flow through), while a WRITE command is gated
    /// per-command on `pause_write_remaining_ms`. This is what makes `CLIENT PAUSE WRITE` genuinely
    /// write-only, matching Redis, while keeping the load-bearing no-write-ack guarantee for the
    /// lossless upgrade write-freeze (#388): a write is never acked while WRITE-paused.
    #[must_use]
    pub fn pause_remaining_ms(&self, now_mono_ms: u64) -> u64 {
        let until = self.pause_until_mono_ms.load(Ordering::Relaxed);
        until.saturating_sub(now_mono_ms)
    }

    /// Whether a pause of ANY kind is ARMED, without reading the clock. `true` iff a pause deadline
    /// has been recorded and not cleared by UNPAUSE; it may already have ELAPSED (the kind-aware
    /// remaining-ms helpers, which take `now`, return `0` past the deadline). This is the cheap
    /// per-command GUARD the serve loop reads BEFORE touching the monotonic clock or classifying the
    /// command: a single relaxed atomic load + compare. On the default (never-paused) path it is
    /// `false` and the per-command write-pause check falls through doing nothing, so the hot path
    /// adds no clock read and no command-classification work.
    #[must_use]
    pub fn is_pause_armed(&self) -> bool {
        self.pause_until_mono_ms.load(Ordering::Relaxed) != 0
    }

    /// Whether the active pause (if any) is WRITE-only.
    #[must_use]
    pub fn pause_is_writes_only(&self) -> bool {
        self.pause_writes_only.load(Ordering::Relaxed)
    }

    /// Whether a pause of ANY kind is active at `now_mono_ms`.
    #[must_use]
    pub fn is_paused(&self, now_mono_ms: u64) -> bool {
        self.pause_remaining_ms(now_mono_ms) > 0
    }

    /// Remaining milliseconds the POST-BATCH serve-loop stall must hold for, given the
    /// monotonic-millis `now`. This is the ALL-pause window ONLY: it returns the remaining ms for an
    /// `ALL` pause (which stalls every command, reads included), and `0` for a WRITE-only pause (so a
    /// WRITE-only pause does NOT hold the whole serve loop -- reads, PING, INFO and SAVE proceed) and
    /// `0` when no pause is active. The serve loop reads THIS in its post-batch stall; a WRITE
    /// command is instead gated per-command on [`Self::pause_write_remaining_ms`]. Relaxed atomics.
    #[must_use]
    pub fn pause_all_remaining_ms(&self, now_mono_ms: u64) -> u64 {
        if self.pause_writes_only.load(Ordering::Relaxed) {
            return 0;
        }
        self.pause_remaining_ms(now_mono_ms)
    }

    /// Remaining milliseconds a WRITE command must stall for, given the monotonic-millis `now`.
    /// A WRITE is blocked by BOTH pause kinds: a WRITE-only pause AND an ALL pause (which also blocks
    /// writes). Returns `0` when no pause is active. The serve loop consults this PER WRITE COMMAND
    /// (after the cheap [`Self::is_pause_armed`] guard) BEFORE dispatching it, so `CLIENT PAUSE <ms>
    /// WRITE` actually stalls writes while reads proceed. Relaxed atomics.
    #[must_use]
    pub fn pause_write_remaining_ms(&self, now_mono_ms: u64) -> u64 {
        self.pause_remaining_ms(now_mono_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn slowlog_records_newest_first_and_caps_len() {
        let sl = SlowLog::with_config(0, 2);
        sl.record(
            1,
            100,
            &raw(&[b"GET", b"a"]),
            "1.1.1.1:1".into(),
            String::new(),
        );
        sl.record(
            2,
            200,
            &raw(&[b"GET", b"b"]),
            "1.1.1.1:2".into(),
            String::new(),
        );
        sl.record(
            3,
            300,
            &raw(&[b"GET", b"c"]),
            "1.1.1.1:3".into(),
            String::new(),
        );
        assert_eq!(sl.len(), 2); // capped at max_len 2
        let got = sl.get(None);
        // Newest first: c (id 2), b (id 1); a was dropped.
        assert_eq!(got[0].args[1], b"c");
        assert_eq!(got[1].args[1], b"b");
        // Ids are monotonic across the drop.
        assert_eq!(got[0].id, 2);
        assert_eq!(got[1].id, 1);
    }

    #[test]
    fn slowlog_get_count_and_reset() {
        let sl = SlowLog::with_config(0, 10);
        for i in 0..5 {
            sl.record(i, 100, &raw(&[b"PING"]), "a".into(), String::new());
        }
        assert_eq!(sl.get(Some(2)).len(), 2);
        assert_eq!(sl.get(Some(100)).len(), 5); // count > len clamps
        assert_eq!(sl.len(), 5);
        sl.reset();
        assert_eq!(sl.len(), 0);
        assert!(sl.is_empty());
    }

    #[test]
    fn slowlog_max_len_zero_keeps_nothing() {
        let sl = SlowLog::with_config(0, 0);
        sl.record(1, 100, &raw(&[b"PING"]), "a".into(), String::new());
        assert_eq!(sl.len(), 0);
    }

    #[test]
    fn slowlog_disabled_threshold_is_negative() {
        let sl = SlowLog::with_config(-1, 128);
        assert!(!sl.enabled());
        sl.set_log_slower_than_micros(0);
        assert!(sl.enabled());
    }

    #[test]
    fn slowlog_entry_caps_arg_count_and_length() {
        let big: Vec<u8> = vec![b'x'; 300];
        let mut many: Vec<Vec<u8>> = Vec::new();
        for _ in 0..40 {
            many.push(big.clone());
        }
        let e = SlowLogEntry::capped(0, 0, 0, &many, String::new(), String::new());
        // 31 kept + 1 synthetic "more arguments" tail = 32.
        assert_eq!(e.args.len(), SLOWLOG_ENTRY_MAX_ARGS + 1);
        // Each kept arg is truncated to the byte cap plus the synthetic suffix.
        assert!(e.args[0].len() > SLOWLOG_ENTRY_MAX_ARG_LEN);
        assert!(e.args[0].starts_with(&[b'x'; SLOWLOG_ENTRY_MAX_ARG_LEN]));
        // The trailing synthetic arg mentions the remaining count.
        assert_eq!(e.args.last().unwrap(), b"... (9 more arguments)");
    }

    #[test]
    fn latency_tracks_worst_and_history_and_reset() {
        let lm = LatencyMonitor::new();
        lm.record("command", 10, 5);
        lm.record("command", 20, 50);
        lm.record("command", 30, 7);
        let latest = lm.latest();
        assert_eq!(latest.len(), 1);
        let (name, ts, worst, max) = &latest[0];
        assert_eq!(name, "command");
        assert_eq!(*ts, 20);
        assert_eq!(*worst, 50);
        assert_eq!(*max, 50);
        // History keeps all three in order.
        assert_eq!(lm.history("command"), vec![(10, 5), (20, 50), (30, 7)]);
        assert_eq!(lm.history("nope"), Vec::<(u64, u64)>::new());
        // Reset by name.
        assert_eq!(lm.reset(&["command".to_owned()]), 1);
        assert!(lm.latest().is_empty());
        // Reset-all on empty returns 0.
        assert_eq!(lm.reset(&[]), 0);
    }

    #[test]
    fn client_registry_register_kill_and_list() {
        let reg = ClientRegistry::new();
        let h1 = reg.register(1, "1.1.1.1:1".into(), "0.0.0.0:6379".into(), 0);
        let _h2 = reg.register(2, "1.1.1.1:2".into(), "0.0.0.0:6379".into(), 0);
        assert_eq!(reg.len(), 2);
        // KILL by id marks the handle.
        assert!(reg.kill_id(1));
        assert!(h1.is_killed());
        assert!(!reg.kill_id(99)); // no match
        // KILL by addr.
        assert!(reg.kill_addr("1.1.1.1:2"));
        assert!(!reg.kill_addr("9.9.9.9:9"));
        // Deregister drops it.
        reg.deregister(1);
        assert_eq!(reg.len(), 1);
        // Snapshot is sorted by id.
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, 2);
    }

    #[test]
    fn client_pause_window() {
        let reg = ClientRegistry::new();
        assert!(!reg.is_paused(1000));
        reg.pause(1000, 500, false);
        assert!(reg.is_paused(1200));
        assert_eq!(reg.pause_remaining_ms(1200), 300);
        assert!(!reg.pause_is_writes_only());
        // Past the deadline: not paused.
        assert!(!reg.is_paused(1600));
        reg.unpause();
        assert!(!reg.is_paused(1100));
    }

    /// THE LOSSLESS-FREEZE NO-WRITE-ACK PROOF (#388), now WRITE-AWARE. A `CLIENT PAUSE <ms> WRITE`
    /// (writes_only = true) sets the SAME `pause_until_mono_ms` deadline an ALL pause does. The
    /// load-bearing guarantee for the upgrade write-freeze is that a WRITE is never ACKED while
    /// paused: the serve loop gates a write command on `pause_write_remaining_ms`, which is non-zero
    /// IN-window for BOTH pause kinds, so no write is dispatched until it elapses. Crucially the
    /// POST-BATCH stall now reads `pause_all_remaining_ms`, which is `0` for a WRITE-only pause -- so
    /// reads + admin (PING/INFO/SAVE) flow through and only writes hold (Redis semantics). The
    /// window expires on its own, and `CLIENT UNPAUSE` clears it immediately.
    #[test]
    fn write_pause_holds_only_writes() {
        let reg = ClientRegistry::new();
        // CLIENT PAUSE 500 WRITE at t=1000.
        reg.pause(1000, 500, true);
        assert!(reg.pause_is_writes_only(), "the pause is WRITE-only");
        assert!(reg.is_pause_armed(), "a recorded pause is armed");
        // A WRITE command stalls IN-window: pause_write_remaining_ms is non-zero, so no write is
        // dispatched (hence never acked) until it elapses -- the upgrade write-freeze guarantee.
        assert!(
            reg.pause_write_remaining_ms(1200) > 0,
            "a WRITE pause holds writes in-window"
        );
        // The POST-BATCH stall reads pause_all_remaining_ms; for a WRITE-only pause it is ZERO, so
        // reads + admin (PING/INFO/SAVE) are NOT held -- this is the read-allowed semantics.
        assert_eq!(
            reg.pause_all_remaining_ms(1200),
            0,
            "a WRITE-only pause does NOT hold the post-batch stall (reads + SAVE proceed)"
        );
        assert!(
            reg.is_paused(1200),
            "is_paused is true for a WRITE pause too"
        );
        // Once the window elapses, writes resume (the new process boots here).
        assert_eq!(
            reg.pause_write_remaining_ms(1600),
            0,
            "the write window self-expires"
        );
        assert!(!reg.is_paused(1600));
        // CLIENT UNPAUSE (the abort-after-freeze un-wedge) clears it immediately, mid-window.
        reg.pause(2000, 500, true);
        assert!(reg.is_paused(2100));
        reg.unpause();
        assert!(!reg.is_pause_armed(), "UNPAUSE disarms the pause");
        assert_eq!(
            reg.pause_write_remaining_ms(2100),
            0,
            "CLIENT UNPAUSE releases the write stall immediately (un-wedge on an aborted upgrade)"
        );
    }

    /// An ALL pause (writes_only = false) holds EVERYTHING via the post-batch stall: both
    /// `pause_all_remaining_ms` (the post-batch read stall, so even a read waits) and
    /// `pause_write_remaining_ms` (writes) are non-zero in-window. This is the unchanged ALL
    /// behavior -- the regression guard that ALL still stalls reads too.
    #[test]
    fn all_pause_holds_reads_and_writes() {
        let reg = ClientRegistry::new();
        reg.pause(1000, 500, false);
        assert!(!reg.pause_is_writes_only(), "the pause is ALL");
        assert!(reg.is_pause_armed());
        assert!(
            reg.pause_all_remaining_ms(1200) > 0,
            "an ALL pause holds the post-batch stall (reads wait too)"
        );
        assert!(
            reg.pause_write_remaining_ms(1200) > 0,
            "an ALL pause also holds writes"
        );
        // Self-expiry.
        assert_eq!(reg.pause_all_remaining_ms(1600), 0);
        assert_eq!(reg.pause_write_remaining_ms(1600), 0);
    }

    /// No pause armed: every kind-aware helper is `0`/`false` and `is_pause_armed` is the single
    /// cheap guard that lets the serve loop's per-command write-pause check fall through for free.
    #[test]
    fn no_pause_is_disarmed_and_zero() {
        let reg = ClientRegistry::new();
        assert!(!reg.is_pause_armed());
        assert_eq!(reg.pause_all_remaining_ms(1234), 0);
        assert_eq!(reg.pause_write_remaining_ms(1234), 0);
        assert!(!reg.is_paused(1234));
    }
}
