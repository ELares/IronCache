// SPDX-License-Identifier: MIT OR Apache-2.0
//! The process-wide runtime-config overlay (CONFIG.md "sources and precedence",
//! the wire `CONFIG SET` layer, #15/#85) and the parameter registry that
//! `CONFIG GET`/`CONFIG SET` dispatch over.
//!
//! ## The runtime overlay is the HIGHEST-precedence layer
//!
//! CONFIG.md orders the layers, highest precedence first: runtime `CONFIG SET` >
//! CLI flags > env > TOML file > built-in defaults. [`RuntimeConfig`] holds the
//! RUNTIME-SETTABLE knobs as that top layer. The lower four layers fold into a
//! [`Config`](crate::Config) at boot; a runtime override then sits ABOVE that
//! resolved value. Structuring it this way is what avoids the reload-clobber bug
//! CONFIG.md calls out: a future file reload refreshes only the file layer (and
//! re-folds the boot `Config`), but a `CONFIG SET` lives in this separate, higher
//! layer, so the reload can never silently clobber it. An operator who wants the
//! file value back clears the runtime layer explicitly (a later `CONFIG SET` to the
//! file value, or a runtime-layer reset).
//!
//! ## Shared-nothing posture (ADR-0002/0005, the freeze-sensitive part)
//!
//! [`RuntimeConfig`] is the ONE new piece of cross-shard shared state PR-4b adds. It
//! is cloned (as an `Arc`) into each shard's `ServerContext` at boot exactly like the
//! shutdown `AtomicBool` precedent, and the per-command HOT-PATH read is a single
//! ACQUIRE atomic load + compare (the [`generation`](RuntimeConfig::generation)),
//! which pairs with the writer's Release bump so the happens-before is carried by the
//! atomic itself.
//! The only lock is the [`std::sync::Mutex`] guarding the policy-name + requirepass
//! strings; it lives HERE in `ironcache-config` (NOT a hot-path crate) and is taken
//! ONLY on the rare `CONFIG SET` / generation-change, never per command. The
//! hot-path crates (ironcache-server/store/eviction/...) stay lock-free: they read
//! `maxmemory` (atomic) and `generation` (atomic) and only reach for the locked
//! strings when the generation says a swap is pending.

use crate::NotifyFlags;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The mutable, cross-shard runtime-config overlay (the highest-precedence layer,
/// CONFIG.md). Shared as `Arc<RuntimeConfig>` into every shard's server context.
///
/// What is ATOMIC vs LOCKED (the shared-nothing contract, ADR-0002):
/// - [`maxmemory`](Self::maxmemory): an [`AtomicU64`]. `CONFIG SET maxmemory` writes
///   it; each shard's admission path reads it (a relaxed load) and recomputes its
///   per-shard budget, so the new ceiling reaches all shards eventually-consistently
///   (within one command each). No lock.
/// - [`generation`](Self::generation): an [`AtomicU64`] bumped on every `CONFIG SET`
///   that changes a HOT-SWAPPABLE param (currently the eviction policy), with a RELEASE
///   store. Each shard keeps its own last-seen generation; the per-command hot-path
///   check is a single ACQUIRE load + compare against that, with NO lock when nothing
///   changed. The Acquire/Release pair carries the happens-before (new name written
///   before the bump that publishes it) in the atomic itself.
/// - The policy NAME string and the `requirepass` string live behind a single
///   [`Mutex`]. The lock is taken ONLY on a `CONFIG SET` that touches them, and on a
///   generation-change policy swap (to read the new name), never on the common
///   per-command path.
#[derive(Debug)]
pub struct RuntimeConfig {
    /// The effective `maxmemory` ceiling in bytes (0 = unlimited). Atomic so a
    /// `CONFIG SET maxmemory` reaches every shard's admission path with a plain
    /// relaxed load (no lock). Seeded from the boot [`Config`](crate::Config).
    maxmemory: AtomicU64,
    /// The hot-swap generation, bumped whenever a hot-swappable param changes (the
    /// eviction policy). Each shard compares its last-seen generation against this
    /// once per command (a relaxed load) and rebuilds its policy only on a change.
    generation: AtomicU64,
    /// The locked strings: the current `maxmemory-policy` name and the `requirepass`
    /// value. Behind ONE `Mutex` that lives in this non-hot-path crate and is taken
    /// only on `CONFIG SET`/generation-change (the per-command hot path never locks).
    strings: Mutex<RuntimeStrings>,
    /// The runtime SAVE POLICY (#58 / durability footgun fix): the periodic-save interval in
    /// SECONDS and the minimum dirty writes a tick requires, BOTH runtime-settable via
    /// `CONFIG SET save "<seconds> <changes> [...]"` (Redis `save` points). `interval_secs == 0`
    /// DISABLES the periodic save (only an explicit SAVE/BGSAVE persists). Seeded from the boot
    /// [`Config`](crate::Config) so a node booted with a configured cadence keeps it, and updated
    /// in place by `CONFIG SET save` so the periodic saver (which reads these atomics each tick)
    /// honors the new policy LIVE -- closing the false-durability footgun where `CONFIG SET save`
    /// was a silent no-op. Two relaxed atomics, node-level cold state read once per (rare) tick,
    /// never on the per-command hot path.
    save_interval_secs: AtomicU64,
    /// The minimum dirty writes a periodic-save tick requires before it fires (the `changes` half
    /// of a Redis `save <seconds> <changes>` point). `0` fires unconditionally on each enabled
    /// tick. Runtime-settable via `CONFIG SET save` (paired with [`Self::save_interval_secs`]).
    save_min_changes: AtomicU64,
    /// Whether the BACKGROUND active-expiry cycle is enabled (Redis `DEBUG SET-ACTIVE-EXPIRE`,
    /// #411). `true` (the default) runs the active drain (per-command + the periodic tick);
    /// `false` makes the active reaper INERT so only LAZY expiry (reap-on-access) removes a key,
    /// which the conformance suites rely on to keep an expired-but-untouched key physically
    /// resident for inspection. Node-level cold state read once per drain (one relaxed load),
    /// never per stored key; lives here (not a process-global) so each server instance toggles
    /// independently and a `DEBUG SET-ACTIVE-EXPIRE` on one connection reaches every shard of THAT
    /// node via the shared `Arc`, exactly like `CONFIG SET maxmemory`.
    active_expire: AtomicBool,
    /// The simultaneous-connection ceiling (Redis `maxclients`, PROD-SAFETY #3). `0` disables the
    /// cap. Runtime-settable via `CONFIG SET maxclients`; the accept path reads it (one relaxed
    /// load) when deciding whether to reject a new connection, so a `CONFIG SET maxclients` takes
    /// effect for subsequent connections without a restart. Seeded from the boot config.
    maxclients: AtomicU64,
    /// The idle client timeout in SECONDS (Redis `timeout`, PROD-SAFETY #4). `0` (the Redis
    /// default) DISABLES idle disconnection (a connection is never closed for being idle).
    /// Runtime-settable via `CONFIG SET timeout`; the serve loop RE-READS it (one relaxed load) at
    /// the top of each connection-loop iteration before the idle wait, so a `CONFIG SET timeout`
    /// takes effect LIVE for already-connected clients (was boot-only -- a change used to require a
    /// restart, dropping every client). Seeded from the boot config.
    timeout_secs: AtomicU64,
    /// The per-connection output-buffer hard cap in bytes (PROD-SAFETY #5). `0` disables the cap.
    /// Runtime-settable via `CONFIG SET output-buffer-limit`; the serve loop reads it (one relaxed
    /// load) after each batch is rendered, so a `CONFIG SET` takes effect for subsequent batches.
    /// Seeded from the boot config.
    output_buffer_limit: AtomicU64,
    /// The per-connection query-buffer hard cap in bytes (#528, the inbound analog of
    /// `output_buffer_limit`). `0` disables the cap. Runtime-settable via
    /// `CONFIG SET query-buffer-limit`; the serve loop reads it (one relaxed load) after each recv
    /// grows the inbound buffer, so a `CONFIG SET` takes effect for subsequent reads. Seeded from
    /// the boot config.
    query_buffer_limit: AtomicU64,
    /// The SLOWLOG threshold in MICROSECONDS (Redis `slowlog-log-slower-than`, PROD-7). `-1`
    /// DISABLES the SLOWLOG (the per-command hook short-circuits on this single load); `0` logs
    /// every command. Signed so `-1` round-trips. Runtime-settable via
    /// `CONFIG SET slowlog-log-slower-than`; the per-command hook + the SLOWLOG command read it.
    /// Seeded from [`crate::DEFAULT_SLOWLOG_LOG_SLOWER_THAN`].
    slowlog_log_slower_than: AtomicI64,
    /// The SLOWLOG max length (Redis `slowlog-max-len`, PROD-7): the maximum retained entries (the
    /// ring drops the oldest past it). Runtime-settable via `CONFIG SET slowlog-max-len`. Seeded
    /// from [`crate::DEFAULT_SLOWLOG_MAX_LEN`].
    slowlog_max_len: AtomicU64,
    /// The `notify-keyspace-events` FLAG BITS (PROD-8 keyspace notifications), stored as the
    /// compact [`crate::NotifyFlags`] bitset. `0` (the default) DISABLES notifications, and the
    /// per-command hot-path read is a single relaxed load the serve loop snapshots into the
    /// shard-local emit gate -- so the default deployment is byte-identical and pays one atomic
    /// load per command. Runtime-settable via `CONFIG SET notify-keyspace-events`; seeded from the
    /// boot config's flag string (parsed + validated at boot).
    notify_keyspace_events: AtomicU32,
    /// The inbound bulk-string + string-value-growth ceiling in bytes (Redis `proto-max-bulk-len`).
    /// Runtime-settable via `CONFIG SET proto-max-bulk-len`; the serve loop builds the decoder
    /// `Limits` from this per connection, and the string/bitmap growth ceilings read it. A single
    /// relaxed load on the (cold) connection-setup / value-growth path, never per command at the
    /// default. Seeded from the boot config (default 512 MB).
    proto_max_bulk_len: AtomicU64,
    /// The TCP keepalive idle interval in SECONDS applied at connection ACCEPT (Redis
    /// `tcp-keepalive`). `0` disables keepalive. Runtime-settable via `CONFIG SET tcp-keepalive`;
    /// the accept path reads it (one relaxed load) so a change applies to newly-accepted
    /// connections. Seeded from the boot config (default 300).
    tcp_keepalive_secs: AtomicU64,
    /// The 8 collection-encoding listpack/intset thresholds (`hash-max-listpack-entries`, ...). The
    /// store reads the live value AT the encoding-transition decision (already a size branch), so
    /// at the compiled default this is one extra relaxed load on the (cold) transition check and
    /// the default encoding behavior is byte-identical. A `CONFIG SET` affects FUTURE inserts only.
    /// `list_max_listpack_size` is the SIGNED Redis form (`-2` etc.); the rest are positive
    /// counts/byte caps. All seeded from the boot config.
    hash_max_listpack_entries: AtomicU64,
    hash_max_listpack_value: AtomicU64,
    list_max_listpack_size: AtomicI64,
    set_max_intset_entries: AtomicU64,
    set_max_listpack_entries: AtomicU64,
    set_max_listpack_value: AtomicU64,
    zset_max_listpack_entries: AtomicU64,
    zset_max_listpack_value: AtomicU64,
}

/// The 8 collection-encoding thresholds the store reads at the encoding-transition decision (#40).
/// Defined in the storage waist (`ironcache-storage`) because that crate -- which holds the
/// `*Value` traits + `Encoding` that consume it -- cannot depend on THIS crate (config already
/// depends on storage; the reverse would be a cycle). Re-exported here so `CONFIG`-side callers can
/// name it through `ironcache_config`. Built from the boot [`Config`] / runtime overlay below.
pub use ironcache_storage::EncodingThresholds;

/// Build the [`EncodingThresholds`] snapshot a boot [`Config`](crate::Config) resolves to (the
/// compiled Redis defaults unless overridden by TOML/env/CLI). The runtime overlay seeds its
/// per-threshold atomics from these; the store seeds its initial snapshot from here too.
#[must_use]
pub fn encoding_thresholds_from_config(cfg: &crate::Config) -> EncodingThresholds {
    EncodingThresholds {
        hash_max_listpack_entries: cfg.hash_max_listpack_entries,
        hash_max_listpack_value: cfg.hash_max_listpack_value,
        list_max_listpack_size: cfg.list_max_listpack_size,
        set_max_intset_entries: cfg.set_max_intset_entries,
        set_max_listpack_entries: cfg.set_max_listpack_entries,
        set_max_listpack_value: cfg.set_max_listpack_value,
        zset_max_listpack_entries: cfg.zset_max_listpack_entries,
        zset_max_listpack_value: cfg.zset_max_listpack_value,
    }
}

/// The string-valued runtime params guarded by [`RuntimeConfig`]'s lock. Grouped
/// under one mutex so a `CONFIG SET` touching both takes a single lock.
#[derive(Debug, Clone)]
struct RuntimeStrings {
    /// The effective `maxmemory-policy` name (verbatim, the configured spelling).
    maxmemory_policy: String,
    /// The effective `requirepass`, stored as the SHA-256 HEX digest AT REST (#65),
    /// NOT the plaintext. Seeded from `Config::requirepass` (already a hash) at boot and
    /// re-hashed from the plaintext on every `CONFIG SET requirepass`. `None` means auth
    /// is not required (an empty value clears it to `None`). No plaintext password ever
    /// lands in this long-lived overlay.
    requirepass: Option<String>,
}

impl RuntimeConfig {
    /// Build the runtime overlay seeded from the boot-resolved [`Config`](crate::Config)
    /// (the fold of CLI > env > TOML > defaults). The runtime layer starts EQUAL to
    /// the resolved values and only diverges when a `CONFIG SET` overrides one; this
    /// is what makes the overlay the highest-precedence layer without duplicating the
    /// lower-layer resolution.
    #[must_use]
    pub fn from_config(cfg: &crate::Config) -> Arc<RuntimeConfig> {
        Arc::new(RuntimeConfig {
            maxmemory: AtomicU64::new(cfg.maxmemory),
            // Generation starts at 0; the first hot-swap bumps it to 1 so a shard's
            // last-seen 0 detects the change.
            generation: AtomicU64::new(0),
            strings: Mutex::new(RuntimeStrings {
                maxmemory_policy: cfg.maxmemory_policy.clone(),
                requirepass: cfg.requirepass.clone(),
            }),
            // Seed the runtime save policy from the boot config so a node started with a
            // configured cadence (`save_interval_secs`/`save_min_changes`) keeps it; a later
            // `CONFIG SET save` then overrides these LIVE (the periodic saver reads them each tick).
            save_interval_secs: AtomicU64::new(cfg.save_interval_secs),
            save_min_changes: AtomicU64::new(cfg.save_min_changes),
            // The connection / output-buffer safety ceilings (PROD-SAFETY #3/#5), seeded from the
            // boot config so a node started with a configured value keeps it; a later `CONFIG SET`
            // overrides these live (the accept path / serve loop read them with a relaxed load).
            maxclients: AtomicU64::new(cfg.maxclients),
            // The idle client timeout (PROD-SAFETY #4), seeded from the boot config so a node
            // started with a configured timeout keeps it; a later `CONFIG SET timeout` overrides it
            // live (the serve loop re-reads it each connection-loop iteration).
            timeout_secs: AtomicU64::new(cfg.timeout_secs),
            output_buffer_limit: AtomicU64::new(cfg.output_buffer_limit),
            // The per-connection query-buffer cap (#528), seeded from the boot config; a later
            // `CONFIG SET query-buffer-limit` overrides it live (the serve loop reads it after each
            // recv grows the inbound buffer).
            query_buffer_limit: AtomicU64::new(cfg.query_buffer_limit),
            // The SLOWLOG knobs (PROD-7), seeded from the boot config so a node started with a
            // configured threshold/length keeps it; a later `CONFIG SET slowlog-*` overrides live.
            active_expire: AtomicBool::new(true),
            slowlog_log_slower_than: AtomicI64::new(cfg.slowlog_log_slower_than),
            slowlog_max_len: AtomicU64::new(cfg.slowlog_max_len),
            // The keyspace-notification flags (PROD-8), seeded from the boot config's flag string.
            // The string was validated in `Config::validate`, so the parse here cannot fail in
            // practice; an EMPTY/invalid string seeds the DISABLED (0) set so the default boot is
            // byte-identical.
            notify_keyspace_events: AtomicU32::new(
                NotifyFlags::parse(&cfg.notify_keyspace_events)
                    .unwrap_or_else(|_| NotifyFlags::empty())
                    .bits(),
            ),
            // The protocol / keepalive ceilings, seeded from the boot config so a configured node
            // keeps its value; a later `CONFIG SET` overrides live (the serve/accept path reads it).
            proto_max_bulk_len: AtomicU64::new(cfg.proto_max_bulk_len),
            tcp_keepalive_secs: AtomicU64::new(cfg.tcp_keepalive_secs),
            // The 8 collection-encoding thresholds, seeded from the boot config (the compiled Redis
            // defaults unless overridden). The store reads these at the encoding-transition decision.
            hash_max_listpack_entries: AtomicU64::new(cfg.hash_max_listpack_entries as u64),
            hash_max_listpack_value: AtomicU64::new(cfg.hash_max_listpack_value as u64),
            list_max_listpack_size: AtomicI64::new(cfg.list_max_listpack_size),
            set_max_intset_entries: AtomicU64::new(cfg.set_max_intset_entries as u64),
            set_max_listpack_entries: AtomicU64::new(cfg.set_max_listpack_entries as u64),
            set_max_listpack_value: AtomicU64::new(cfg.set_max_listpack_value as u64),
            zset_max_listpack_entries: AtomicU64::new(cfg.zset_max_listpack_entries as u64),
            zset_max_listpack_value: AtomicU64::new(cfg.zset_max_listpack_value as u64),
        })
    }

    /// The current effective `maxmemory` ceiling in bytes (0 = unlimited). A single
    /// relaxed atomic load: this is the per-command read the admission path makes, so
    /// it must stay lock-free.
    #[must_use]
    pub fn maxmemory(&self) -> u64 {
        self.maxmemory.load(Ordering::Relaxed)
    }

    /// The current hot-swap generation. A single ACQUIRE atomic load: this is the
    /// per-command hot-path check each shard compares against its last-seen value, so
    /// it must stay lock-free. The Acquire pairs with the writer's Release bump in
    /// [`Self::set_policy_name`], so the happens-before (the new policy name was written
    /// before the generation bump that publishes it) is carried by the atomic ITSELF,
    /// robustly, independent of the strings Mutex. An uncontended Acquire load is free
    /// on the common path (no fence on x86; a plain `ldar` on aarch64).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The current effective `maxmemory-policy` name (a clone of the locked string).
    /// Takes the lock, so it is called only off the hot path (a `CONFIG GET`, an INFO
    /// render, or a generation-change policy swap), NEVER per command.
    #[must_use]
    pub fn policy_name(&self) -> String {
        self.strings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .maxmemory_policy
            .clone()
    }

    /// The current effective `requirepass` as the stored SHA-256 HEX digest (a clone of
    /// the locked value, `None` if auth is not required), NOT the plaintext (#65). Takes
    /// the lock; called off the hot path (a `CONFIG GET`, or the auth check, which is
    /// rare relative to data commands). The auth path hashes the provided guess and
    /// compares against this digest.
    #[must_use]
    pub fn requirepass(&self) -> Option<String> {
        self.strings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requirepass
            .clone()
    }

    /// Whether auth is currently required (a non-empty `requirepass`). Convenience over
    /// [`Self::requirepass`]; still takes the lock, so it is off the hot path.
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        self.requirepass().is_some()
    }

    /// `CONFIG SET maxmemory <bytes>`: store the new ceiling (a relaxed atomic store).
    /// No generation bump: the admission path reads `maxmemory()` directly every time
    /// it checks the ceiling, so the new value reaches all shards without a policy
    /// rebuild.
    pub fn set_maxmemory(&self, bytes: u64) {
        self.maxmemory.store(bytes, Ordering::Relaxed);
    }

    /// `CONFIG SET maxmemory-policy <name>`: update the locked name and BUMP the
    /// generation so every shard notices and rebuilds its policy. Returns the new
    /// generation (for tests / introspection). Takes the lock (rare, off the hot path).
    pub fn set_policy_name(&self, name: &str) -> u64 {
        {
            let mut s = self
                .strings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            name.clone_into(&mut s.maxmemory_policy);
        }
        // Bump the generation AFTER writing the name with a RELEASE store, which pairs
        // with the per-command ACQUIRE load in `generation()`: a shard that observes the
        // new generation value is therefore guaranteed to also see the new name written
        // above. The Release/Acquire pair carries this happens-before in the atomic
        // itself, so it holds even though the name lives behind a separate Mutex (the
        // shard still re-reads the name under the lock; the ordering just guarantees the
        // bump is not reordered ahead of the name write). `fetch_add` returns the
        // PREVIOUS value; the new generation is +1.
        self.generation.fetch_add(1, Ordering::Release) + 1
    }

    /// `CONFIG SET requirepass <value>`: HASH the PLAINTEXT `value` to its SHA-256 hex
    /// digest and store ONLY that (#65), so no plaintext password lands in the long-lived
    /// overlay. The `value` is always a PLAINTEXT password (Redis `requirepass`
    /// semantics); the ACL `#<hash>` pre-hashed syntax is #106 (later), so a digest read
    /// back via `CONFIG GET requirepass` is NOT meant to be re-`SET` (doing so would hash
    /// the hash). An empty value CLEARS it (`None`, disabling auth), matching Redis. No
    /// generation bump: the auth path reads `requirepass()` directly. Takes the lock
    /// (rare, off the hot path).
    pub fn set_requirepass(&self, value: &str) {
        // Hash the plaintext BEFORE taking the lock so the transient plaintext is dropped
        // promptly; the locked overlay only ever sees the digest.
        let hashed = if value.is_empty() {
            None
        } else {
            Some(crate::sha256_hex(value.as_bytes()))
        };
        let mut s = self
            .strings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.requirepass = hashed;
    }

    /// The CURRENT runtime save policy: `(interval_secs, min_changes)` (#58 durability footgun
    /// fix). `interval_secs == 0` means the periodic save is DISABLED. The periodic saver reads
    /// this each tick (two relaxed atomic loads) so a `CONFIG SET save` takes effect LIVE.
    #[must_use]
    pub fn save_policy(&self) -> (u64, u64) {
        (
            self.save_interval_secs.load(Ordering::Relaxed),
            self.save_min_changes.load(Ordering::Relaxed),
        )
    }

    /// Whether a periodic save CADENCE is currently configured (a non-zero interval). The analog
    /// of a Redis `save <secs> <changes>` save point being set; read by the shutdown save-on-exit
    /// decision and the INFO `# Persistence` `rdb_*` policy line.
    #[must_use]
    pub fn has_save_policy(&self) -> bool {
        self.save_interval_secs.load(Ordering::Relaxed) > 0
    }

    /// The current effective `maxclients` ceiling (PROD-SAFETY #3); `0` disables the cap. A
    /// single relaxed atomic load: the per-connection ACCEPT path reads it once when deciding
    /// whether to reject a new connection (a cold accept-path read, NOT per command).
    #[must_use]
    pub fn maxclients(&self) -> u64 {
        self.maxclients.load(Ordering::Relaxed)
    }

    /// `CONFIG SET maxclients <n>`: store the new connection ceiling (a relaxed atomic store);
    /// `0` disables the cap. The accept path reads `maxclients()` directly per new connection, so
    /// the new value applies to subsequent connections without a restart (PROD-SAFETY #3).
    pub fn set_maxclients(&self, n: u64) {
        self.maxclients.store(n, Ordering::Relaxed);
    }

    /// Whether the BACKGROUND active-expiry cycle is enabled (`DEBUG SET-ACTIVE-EXPIRE`, #411).
    /// `true` by default; the active drain (per-command + periodic tick) reads this once and
    /// skips reaping when it is `false` (lazy reap-on-access still applies). One relaxed load.
    #[must_use]
    pub fn active_expire_enabled(&self) -> bool {
        self.active_expire.load(Ordering::Relaxed)
    }

    /// Enable or disable the background active-expiry cycle (`DEBUG SET-ACTIVE-EXPIRE 1|0`, #411).
    /// Reaches every shard of this node via the shared `Arc` (one relaxed store).
    pub fn set_active_expire(&self, on: bool) {
        self.active_expire.store(on, Ordering::Relaxed);
    }

    /// The current effective idle client timeout in SECONDS (Redis `timeout`, PROD-SAFETY #4); `0`
    /// disables idle disconnection. A single relaxed atomic load: the serve loop re-reads it at the
    /// top of each connection-loop iteration (a cold, post-batch read, NOT per command) to compute
    /// the fresh idle deadline.
    #[must_use]
    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs.load(Ordering::Relaxed)
    }

    /// `CONFIG SET timeout <secs>`: store the new idle timeout (a relaxed atomic store); `0`
    /// disables idle disconnection. The serve loop reads `timeout_secs()` each connection-loop
    /// iteration, so the new value applies LIVE to already-connected clients on their next idle wait
    /// (a non-zero<->0 change switches between the timer-select arm and the plain-recv arm).
    pub fn set_timeout_secs(&self, secs: u64) {
        self.timeout_secs.store(secs, Ordering::Relaxed);
    }

    /// The current effective per-connection output-buffer hard cap in bytes (PROD-SAFETY #5);
    /// `0` disables the cap. A single relaxed atomic load: the serve loop reads it after each
    /// rendered batch (off the per-command decode/dispatch hot path).
    #[must_use]
    pub fn output_buffer_limit(&self) -> u64 {
        self.output_buffer_limit.load(Ordering::Relaxed)
    }

    /// `CONFIG SET output-buffer-limit <bytes>`: store the new per-connection output-buffer cap
    /// (a relaxed atomic store); `0` disables it. The serve loop reads `output_buffer_limit()`
    /// after each rendered batch, so the new value applies to subsequent batches (PROD-SAFETY #5).
    pub fn set_output_buffer_limit(&self, bytes: u64) {
        self.output_buffer_limit.store(bytes, Ordering::Relaxed);
    }

    /// The current effective per-connection query-buffer hard cap in bytes (#528, the inbound
    /// analog of [`output_buffer_limit`](Self::output_buffer_limit)); `0` disables the cap. A
    /// single relaxed atomic load: the serve loop reads it after each recv grows the inbound
    /// buffer (off the per-command decode/dispatch hot path).
    #[must_use]
    pub fn query_buffer_limit(&self) -> u64 {
        self.query_buffer_limit.load(Ordering::Relaxed)
    }

    /// `CONFIG SET query-buffer-limit <bytes>`: store the new per-connection query-buffer cap
    /// (a relaxed atomic store); `0` disables it. The serve loop reads `query_buffer_limit()`
    /// after each recv grows the inbound buffer, so the new value applies to subsequent reads
    /// (#528).
    pub fn set_query_buffer_limit(&self, bytes: u64) {
        self.query_buffer_limit.store(bytes, Ordering::Relaxed);
    }

    /// `CONFIG SET save "<seconds> <changes> [...]"`: REPLACE the runtime save policy (#58
    /// durability footgun fix -- previously a silent no-op that lied about durability). Stores the
    /// SHORTEST (most aggressive) save point's `seconds`/`changes` as the live interval/min-changes
    /// the periodic saver reads, mirroring how the cache collapses Redis's multiple save points to
    /// one periodic cadence. An EMPTY string DISABLES the periodic save (interval 0), matching
    /// `CONFIG SET save ""`. Two relaxed atomic stores (rare, off the hot path); the periodic saver
    /// picks up the new policy on its next tick. The caller validated the points via
    /// [`crate::parse_save_points`].
    pub fn set_save_policy(&self, interval_secs: u64, min_changes: u64) {
        self.save_interval_secs
            .store(interval_secs, Ordering::Relaxed);
        self.save_min_changes.store(min_changes, Ordering::Relaxed);
    }

    /// The current `slowlog-log-slower-than` threshold in MICROSECONDS (PROD-7); `-1` disables the
    /// SLOWLOG, `0` logs everything. A single relaxed load: the per-command timing hook reads it
    /// first and short-circuits when it is `-1`.
    #[must_use]
    pub fn slowlog_log_slower_than(&self) -> i64 {
        self.slowlog_log_slower_than.load(Ordering::Relaxed)
    }

    /// `CONFIG SET slowlog-log-slower-than <micros>`: store the new threshold (a relaxed store).
    /// `-1` disables the SLOWLOG. The per-command hook + the SLOWLOG command read it directly.
    pub fn set_slowlog_log_slower_than(&self, micros: i64) {
        self.slowlog_log_slower_than
            .store(micros, Ordering::Relaxed);
    }

    /// The current `slowlog-max-len` (PROD-7): the maximum retained SLOWLOG entries. A single
    /// relaxed load.
    #[must_use]
    pub fn slowlog_max_len(&self) -> u64 {
        self.slowlog_max_len.load(Ordering::Relaxed)
    }

    /// `CONFIG SET slowlog-max-len <n>`: store the new max length (a relaxed store). The SLOWLOG
    /// ring is trimmed to it on its next push (and immediately by the command layer, which mirrors
    /// the value into the live `SlowLog`).
    pub fn set_slowlog_max_len(&self, n: u64) {
        self.slowlog_max_len.store(n, Ordering::Relaxed);
    }

    /// The current `notify-keyspace-events` flag set (PROD-8). A single RELAXED atomic load: the
    /// serve loop reads it once per command (snapshotting it into the shard-local emit gate). `0`
    /// (the default) is the DISABLED set, so the emit gate short-circuits and the default
    /// deployment is byte-identical.
    #[must_use]
    pub fn notify_flags(&self) -> NotifyFlags {
        NotifyFlags::from_bits(self.notify_keyspace_events.load(Ordering::Relaxed))
    }

    /// `CONFIG SET notify-keyspace-events <flags>`: store the new flag set (a relaxed store). The
    /// serve loop reads `notify_flags()` once per command, so the new flags take effect for
    /// subsequent commands without a restart. The caller (the registry's runtime setter) parsed +
    /// validated the flag string before calling this.
    pub fn set_notify_flags(&self, flags: NotifyFlags) {
        self.notify_keyspace_events
            .store(flags.bits(), Ordering::Relaxed);
    }

    /// The current effective `proto-max-bulk-len` in bytes (default 512 MB). A single relaxed
    /// atomic load: the serve loop reads it when building the per-connection decoder `Limits`, and
    /// the string/bitmap growth ceilings read it when checking a value-growth edit (both cold paths
    /// relative to the per-command decode/dispatch).
    #[must_use]
    pub fn proto_max_bulk_len(&self) -> u64 {
        self.proto_max_bulk_len.load(Ordering::Relaxed)
    }

    /// `CONFIG SET proto-max-bulk-len <bytes>`: store the new ceiling (a relaxed store). The serve
    /// loop reads it when (re)building a connection's decoder `Limits`, and the string/bitmap
    /// ceilings read it directly, so a change applies to subsequent connections + subsequent
    /// value-growth edits. The caller validated the byte size (non-zero).
    pub fn set_proto_max_bulk_len(&self, bytes: u64) {
        self.proto_max_bulk_len.store(bytes, Ordering::Relaxed);
    }

    /// The current effective `tcp-keepalive` idle interval in SECONDS (default 300); `0` disables
    /// keepalive. A single relaxed load the accept path reads per new connection.
    #[must_use]
    pub fn tcp_keepalive_secs(&self) -> u64 {
        self.tcp_keepalive_secs.load(Ordering::Relaxed)
    }

    /// `CONFIG SET tcp-keepalive <secs>`: store the new keepalive idle interval (a relaxed store);
    /// `0` disables it. The accept path reads it directly, so a change applies to NEWLY-accepted
    /// connections (existing connections keep the option set at their own accept time, matching
    /// Redis).
    pub fn set_tcp_keepalive_secs(&self, secs: u64) {
        self.tcp_keepalive_secs.store(secs, Ordering::Relaxed);
    }

    /// A SNAPSHOT of the 8 collection-encoding thresholds (the value the store reads at the
    /// encoding-transition decision). Taken off the hot path: the store refreshes its own cached
    /// snapshot only when the runtime [`generation`](Self::generation) moves (the SAME per-command
    /// generation check the policy hot-swap uses), so the per-edit decision is a plain field read on
    /// the store's cached copy, never an atomic per edit.
    #[must_use]
    pub fn encoding_thresholds(&self) -> EncodingThresholds {
        EncodingThresholds {
            hash_max_listpack_entries: self.hash_max_listpack_entries.load(Ordering::Relaxed)
                as usize,
            hash_max_listpack_value: self.hash_max_listpack_value.load(Ordering::Relaxed) as usize,
            list_max_listpack_size: self.list_max_listpack_size.load(Ordering::Relaxed),
            set_max_intset_entries: self.set_max_intset_entries.load(Ordering::Relaxed) as usize,
            set_max_listpack_entries: self.set_max_listpack_entries.load(Ordering::Relaxed)
                as usize,
            set_max_listpack_value: self.set_max_listpack_value.load(Ordering::Relaxed) as usize,
            zset_max_listpack_entries: self.zset_max_listpack_entries.load(Ordering::Relaxed)
                as usize,
            zset_max_listpack_value: self.zset_max_listpack_value.load(Ordering::Relaxed) as usize,
        }
    }

    /// Store ONE collection-encoding threshold by its registered param name and BUMP the generation
    /// (a Release store) so every shard refreshes its cached [`EncodingThresholds`] snapshot on its
    /// next command -- the SAME generation funnel the policy hot-swap rides, so no new per-command
    /// read is added. Returns `true` if `name` is a recognized threshold (the caller validated the
    /// numeric value first), `false` otherwise (a programming error the registry surfaces). The
    /// generation bump pairs with the per-command Acquire load in [`Self::generation`], so a shard
    /// that observes the new generation is guaranteed to see the new threshold value written here.
    pub fn set_encoding_threshold(&self, name: &str, value: i64) -> bool {
        match name {
            "hash-max-listpack-entries" => {
                self.hash_max_listpack_entries
                    .store(value as u64, Ordering::Relaxed);
            }
            "hash-max-listpack-value" => {
                self.hash_max_listpack_value
                    .store(value as u64, Ordering::Relaxed);
            }
            "list-max-listpack-size" => {
                self.list_max_listpack_size.store(value, Ordering::Relaxed);
            }
            "set-max-intset-entries" => {
                self.set_max_intset_entries
                    .store(value as u64, Ordering::Relaxed);
            }
            "set-max-listpack-entries" => {
                self.set_max_listpack_entries
                    .store(value as u64, Ordering::Relaxed);
            }
            "set-max-listpack-value" => {
                self.set_max_listpack_value
                    .store(value as u64, Ordering::Relaxed);
            }
            "zset-max-listpack-entries" => {
                self.zset_max_listpack_entries
                    .store(value as u64, Ordering::Relaxed);
            }
            "zset-max-listpack-value" => {
                self.zset_max_listpack_value
                    .store(value as u64, Ordering::Relaxed);
            }
            _ => return false,
        }
        // Publish the new threshold via a Release generation bump (pairs with the per-command
        // Acquire load): a shard observing the new generation refreshes its cached snapshot and is
        // guaranteed to see the value stored above.
        self.generation.fetch_add(1, Ordering::Release);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    #[test]
    fn seeds_from_config_and_reads_back() {
        // `Config::requirepass` already holds the SHA-256 HEX at rest (#65), so
        // from_config copies that digest verbatim (it does NOT re-hash). We seed it with
        // a digest to mirror what a real boot produces.
        let stored = crate::sha256_hex(b"pw");
        let cfg = Config {
            maxmemory: 1024,
            maxmemory_policy: "allkeys-lru".to_owned(),
            requirepass: Some(stored.clone()),
            ..Config::default()
        };
        let rc = RuntimeConfig::from_config(&cfg);
        assert_eq!(rc.maxmemory(), 1024);
        assert_eq!(rc.policy_name(), "allkeys-lru");
        // The runtime overlay holds the same digest the Config did (no plaintext).
        assert_eq!(rc.requirepass().as_deref(), Some(stored.as_str()));
        assert!(rc.requires_auth());
        // Generation starts at 0 (no hot-swap yet).
        assert_eq!(rc.generation(), 0);
    }

    #[test]
    fn set_maxmemory_is_lock_free_visible() {
        let rc = RuntimeConfig::from_config(&Config::default());
        rc.set_maxmemory(4096);
        assert_eq!(rc.maxmemory(), 4096);
        // No generation bump for maxmemory (admission reads it directly).
        assert_eq!(rc.generation(), 0);
    }

    #[test]
    fn set_policy_name_bumps_generation() {
        let rc = RuntimeConfig::from_config(&Config::default());
        let g0 = rc.generation();
        let g1 = rc.set_policy_name("allkeys-lfu");
        assert_eq!(g1, g0 + 1);
        assert_eq!(rc.generation(), g1);
        assert_eq!(rc.policy_name(), "allkeys-lfu");
        // A second swap bumps again.
        let g2 = rc.set_policy_name("volatile-ttl");
        assert_eq!(g2, g1 + 1);
        assert_eq!(rc.policy_name(), "volatile-ttl");
    }

    #[test]
    fn set_timeout_secs_round_trips() {
        // The idle timeout (PROD-SAFETY #4) is runtime-settable: it seeds from the boot config and
        // a setter overrides it live (the serve loop re-reads it each iteration). `0` disables it.
        let cfg = Config {
            timeout_secs: 45,
            ..Config::default()
        };
        let rc = RuntimeConfig::from_config(&cfg);
        // Seeded from the boot config.
        assert_eq!(rc.timeout_secs(), 45);
        // A setter overrides it.
        rc.set_timeout_secs(30);
        assert_eq!(rc.timeout_secs(), 30);
        // `0` disables idle disconnection.
        rc.set_timeout_secs(0);
        assert_eq!(rc.timeout_secs(), 0);
        // The default boot config seeds `0` (Redis default: idle disconnection off).
        let rc_default = RuntimeConfig::from_config(&Config::default());
        assert_eq!(rc_default.timeout_secs(), 0);
    }

    #[test]
    fn set_query_buffer_limit_round_trips() {
        let cfg = Config {
            query_buffer_limit: 4096,
            ..Config::default()
        };
        let rc = RuntimeConfig::from_config(&cfg);
        // Seeded from the boot config.
        assert_eq!(rc.query_buffer_limit(), 4096);
        // A setter overrides it.
        rc.set_query_buffer_limit(8192);
        assert_eq!(rc.query_buffer_limit(), 8192);
        // `0` disables the cap.
        rc.set_query_buffer_limit(0);
        assert_eq!(rc.query_buffer_limit(), 0);
        // The default seeds the high 1 GiB ceiling (mirrors the output cap).
        let rc_default = RuntimeConfig::from_config(&Config::default());
        assert_eq!(
            rc_default.query_buffer_limit(),
            crate::DEFAULT_QUERY_BUFFER_LIMIT
        );
    }

    #[test]
    fn set_proto_max_bulk_len_round_trips() {
        let cfg = Config {
            proto_max_bulk_len: 1024,
            ..Config::default()
        };
        let rc = RuntimeConfig::from_config(&cfg);
        assert_eq!(rc.proto_max_bulk_len(), 1024);
        rc.set_proto_max_bulk_len(2048);
        assert_eq!(rc.proto_max_bulk_len(), 2048);
        // The default seeds the Redis 512 MB ceiling.
        let rc_default = RuntimeConfig::from_config(&Config::default());
        assert_eq!(
            rc_default.proto_max_bulk_len(),
            crate::DEFAULT_PROTO_MAX_BULK_LEN
        );
    }

    #[test]
    fn set_tcp_keepalive_round_trips() {
        let cfg = Config {
            tcp_keepalive_secs: 120,
            ..Config::default()
        };
        let rc = RuntimeConfig::from_config(&cfg);
        assert_eq!(rc.tcp_keepalive_secs(), 120);
        rc.set_tcp_keepalive_secs(0);
        assert_eq!(rc.tcp_keepalive_secs(), 0);
        // The default seeds the Redis 300 s interval.
        let rc_default = RuntimeConfig::from_config(&Config::default());
        assert_eq!(
            rc_default.tcp_keepalive_secs(),
            crate::DEFAULT_TCP_KEEPALIVE
        );
    }

    #[test]
    fn encoding_thresholds_seed_and_bump_generation() {
        let rc = RuntimeConfig::from_config(&Config::default());
        // Seeded from the compiled defaults (the byte-identical default deployment).
        let t = rc.encoding_thresholds();
        assert_eq!(t, EncodingThresholds::defaults());
        // Setting a threshold bumps the generation so shards refresh their snapshot.
        let g0 = rc.generation();
        assert!(rc.set_encoding_threshold("hash-max-listpack-entries", 4));
        assert_eq!(rc.generation(), g0 + 1);
        assert_eq!(rc.encoding_thresholds().hash_max_listpack_entries, 4);
        // The signed list form round-trips.
        assert!(rc.set_encoding_threshold("list-max-listpack-size", -5));
        assert_eq!(rc.encoding_thresholds().list_max_listpack_size, -5);
        // An unrecognized threshold name does not bump the generation.
        let g1 = rc.generation();
        assert!(!rc.set_encoding_threshold("nonsense", 1));
        assert_eq!(rc.generation(), g1);
    }

    #[test]
    fn storage_defaults_match_config_defaults() {
        // The storage waist's `EncodingThresholds::defaults()` uses literals (it cannot reference
        // the config constants without a dependency cycle); assert they agree with the values a
        // default `Config` resolves to, so the two single-sources can never silently drift.
        assert_eq!(
            EncodingThresholds::defaults(),
            encoding_thresholds_from_config(&Config::default())
        );
    }

    #[test]
    fn encoding_thresholds_unlimited_never_converts() {
        let u = EncodingThresholds::unlimited();
        assert_eq!(u.hash_max_listpack_entries, usize::MAX);
        assert_eq!(u.set_max_intset_entries, usize::MAX);
        // The list budget resolves to the 64 KB tier with no entry cap.
        assert_eq!(u.list_budget(), (64 * 1024, usize::MAX));
    }

    #[test]
    fn set_requirepass_hashes_at_rest_and_empty_clears_auth() {
        let rc = RuntimeConfig::from_config(&Config::default());
        // SECURITY (#65): CONFIG SET requirepass takes a PLAINTEXT password and stores
        // ONLY its SHA-256 hex digest; the plaintext never lands in the overlay.
        rc.set_requirepass("secret");
        let stored = rc.requirepass().expect("requirepass should be set");
        assert_eq!(stored, crate::sha256_hex(b"secret"));
        assert_ne!(stored, "secret");
        assert!(rc.requires_auth());
        // Empty string disables auth (Redis parity).
        rc.set_requirepass("");
        assert_eq!(rc.requirepass(), None);
        assert!(!rc.requires_auth());
    }
}
