// SPDX-License-Identifier: MIT OR Apache-2.0
//! The `CONFIG GET`/`CONFIG SET` parameter registry (CONFIG.md "wire parity",
//! ADR-0009, #15/#85).
//!
//! A [`ParamSpec`] table maps each Redis-recognized `maxmemory-policy`/`maxmemory`/
//! ... parameter name to: a GETTER that produces the current EFFECTIVE value as a
//! string (so `CONFIG GET` globs over names and returns name->value pairs), an
//! optional kind that says whether the param is RUNTIME-SETTABLE (and how to apply
//! the set), a NO-OP-but-recognized accepted param, or RESTART-REQUIRED (read-only at
//! runtime, reported with the Redis-style "can't set at runtime" error).
//!
//! ## Effective-value resolution (the precedence fold)
//!
//! Each getter resolves the effective value from the [`RuntimeConfig`] overlay
//! (highest precedence) where the param is runtime-settable, falling through to the
//! boot-resolved [`Config`] (the fold of CLI > env > TOML > defaults) for the
//! restart-required params. Because the runtime-settable getters read the overlay
//! FIRST, a `CONFIG SET` value out-ranks the file/CLI/env/default value it was
//! resolved from, which is the precedence CONFIG.md mandates and the reload-clobber
//! avoidance (the overlay is a separate, higher layer; a file reload cannot touch it).

use crate::runtime::RuntimeConfig;
use crate::{Config, MAXMEMORY_POLICY_NAMES, parse_human_size};

/// The outcome of a single `CONFIG SET name value` application.
#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    /// The value was applied (the param is runtime-settable or an accepted no-op).
    Applied,
    /// The param name is not in the registry. The caller emits the canonical
    /// `ERR Unknown option ...` error.
    UnknownParam,
    /// The param exists but cannot be set at runtime (restart-required). The caller
    /// emits the Redis-style "can't set ... at runtime" error.
    RestartRequired,
    /// The param exists and is settable, but the value was rejected (e.g. a malformed
    /// `maxmemory` size, or an unrecognized `maxmemory-policy` name). The string is the
    /// reason, surfaced in the caller's `CONFIG SET failed` error.
    InvalidValue(String),
    /// The param is recognized but the underlying feature is NOT SUPPORTED by this build, so a
    /// `CONFIG SET` is REFUSED with an explicit error rather than silently accepted (the
    /// false-durability footgun fix for `appendonly`: IronCache has no AOF). The string is the
    /// operator-facing reason the caller surfaces verbatim.
    Unsupported(String),
}

/// How a registered parameter behaves under `CONFIG SET` (CONFIG.md hot-swappable vs
/// restart-required vs accepted-no-op partition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetKind {
    /// Runtime-settable: `CONFIG SET` applies it to the [`RuntimeConfig`] overlay.
    Runtime,
    /// Accepted but a no-op under IronCache's engine (e.g. `maxmemory-samples`,
    /// `save`, `appendonly`): `CONFIG SET` replies `+OK` and the value is echoed by
    /// `CONFIG GET`, but nothing changes (CONFIG.md "accepted and echoed for
    /// compatibility, documented as no-ops"). The no-op value is NOT stored (the
    /// getter returns a fixed Redis-recognized default), matching how Redis surfaces
    /// these under a non-persistence cache build.
    AcceptedNoOp,
    /// Recognized but the underlying feature is NOT SUPPORTED by this build, so turning the
    /// feature ON via `CONFIG SET` is REFUSED with an explicit error (the false-durability
    /// footgun fix). VALUE-SENSITIVE: turning the feature OFF is a no-op-OK. Currently ONLY
    /// `appendonly`: IronCache persists via SNAPSHOTS (SAVE/BGSAVE + the `save` cadence), it has
    /// no AOF, so `CONFIG SET appendonly yes` must NOT be silently accepted (an operator would
    /// believe AOF durability is on when it is not), but `CONFIG SET appendonly no` replies +OK
    /// (the feature is already off; a client / ops tool that sets `appendonly no` at startup
    /// expects success, matching Redis). `CONFIG GET appendonly` always reports `no`.
    Unsupported,
    /// Restart-required: read-only at runtime (bind/port/databases/io-threads/shards).
    /// `CONFIG SET` returns the Redis-style can't-set-at-runtime error rather than
    /// silently ignoring it (CONFIG.md "reported as requiring a restart rather than
    /// silently ignored").
    ///
    /// Two DISTINCT reasons land here, and they do NOT all mirror Redis:
    /// - `databases` and `io-threads` are genuinely IMMUTABLE in BOTH IronCache and
    ///   Redis (Redis marks them `IMMUTABLE_CONFIG`): they cannot change at runtime in
    ///   either system, so reporting restart-required matches Redis.
    /// - `bind` and `port` are MODIFIABLE_CONFIG in Redis (accepted at runtime; Redis
    ///   re-binds the listening socket). IronCache reports them restart-required as a
    ///   DELIBERATE DIVERGENCE: under the thread-per-core boot model the listening
    ///   sockets are bound once at startup and cannot be re-bound / re-ported live, so a
    ///   runtime set would be a silent lie. We reject with the restart-required error
    ///   rather than pretend it took effect. (Re-bind-at-runtime is a possible future
    ///   capability; until then this is the faithful behavior.)
    RestartRequired,
}

/// One registered `CONFIG` parameter (CONFIG.md parameter registry). The getter and
/// setter close over the runtime overlay + boot config so `CONFIG GET`/`SET` need no
/// per-param match in the command layer.
pub struct ParamSpec {
    /// The Redis-recognized parameter name (lowercase, the `CONFIG GET`/`SET` token).
    pub name: &'static str,
    /// How `CONFIG SET` treats this param.
    pub kind: SetKind,
}

/// The set of registered parameter names (CONFIG.md). Returned in a stable order so
/// `CONFIG GET *` is deterministic. The list is intentionally small for PR-4b: the
/// runtime-settable trio (`maxmemory`, `maxmemory-policy`, `requirepass`), the
/// accepted no-ops (`maxmemory-samples`, `save`, `appendonly`), and the
/// restart-required read-only-reported ones (`bind`, `port`, `databases`, and the
/// shard count under the Redis-recognized `io-threads` name plus the native
/// `shards`). New params are added here as their subsystems land.
#[must_use]
pub fn param_specs() -> &'static [ParamSpec] {
    &[
        ParamSpec {
            name: "maxmemory",
            kind: SetKind::Runtime,
        },
        ParamSpec {
            name: "maxmemory-policy",
            kind: SetKind::Runtime,
        },
        ParamSpec {
            name: "requirepass",
            kind: SetKind::Runtime,
        },
        // The simultaneous-connection ceiling (Redis `maxclients`, PROD-SAFETY #3). RUNTIME-
        // SETTABLE: `CONFIG SET maxclients <n>` updates the live ceiling the accept path reads,
        // matching Redis (`maxclients` is a MODIFIABLE config). `0` disables the cap.
        ParamSpec {
            name: "maxclients",
            kind: SetKind::Runtime,
        },
        // The per-connection output-buffer hard cap in bytes (PROD-SAFETY #5, the IronCache analog
        // of Redis `client-output-buffer-limit`). RUNTIME-SETTABLE: `CONFIG SET output-buffer-limit
        // <bytes>` updates the live cap the serve loop enforces; `0` disables it. Named with the
        // simple scalar `output-buffer-limit` (IronCache uses a single hard byte cap, not Redis's
        // per-class `<class> <hard> <soft> <secs>` grammar).
        ParamSpec {
            name: "output-buffer-limit",
            kind: SetKind::Runtime,
        },
        ParamSpec {
            name: "maxmemory-samples",
            kind: SetKind::AcceptedNoOp,
        },
        // The SLOWLOG knobs (PROD-7). RUNTIME-SETTABLE: `CONFIG SET slowlog-log-slower-than <micros>`
        // (`-1` disables the SLOWLOG; `0` logs everything) and `CONFIG SET slowlog-max-len <n>`
        // update the live SLOWLOG the per-command timing hook + the SLOWLOG command read.
        ParamSpec {
            name: "slowlog-log-slower-than",
            kind: SetKind::Runtime,
        },
        ParamSpec {
            name: "slowlog-max-len",
            kind: SetKind::Runtime,
        },
        // Keyspace notifications (PROD-8). RUNTIME-SETTABLE: `CONFIG SET notify-keyspace-events
        // <flags>` parses the flag string (`KEA...`) into the live overlay the serve loop reads;
        // `CONFIG GET notify-keyspace-events` renders the canonical flag string. The empty string
        // disables notifications (the default).
        ParamSpec {
            name: "notify-keyspace-events",
            kind: SetKind::Runtime,
        },
        // `save` is RUNTIME-SETTABLE (#58 durability footgun fix): `CONFIG SET save "<seconds>
        // <changes>"` ACTUALLY updates the periodic save policy the saver reads, and `CONFIG GET
        // save` reports the REAL policy -- no longer a silent no-op that lies about durability.
        ParamSpec {
            name: "save",
            kind: SetKind::Runtime,
        },
        // `appendonly` is UNSUPPORTED (#58 durability footgun fix): IronCache has NO AOF (it
        // persists via snapshots), so `CONFIG SET appendonly yes` is REFUSED with an explicit
        // error instead of silently accepted; `CONFIG GET appendonly` reports `no`.
        ParamSpec {
            name: "appendonly",
            kind: SetKind::Unsupported,
        },
        // The list listpack->quicklist threshold (PR-5, #40). Recognized + echoed for
        // compatibility; the store reads its own resolved byte-budget default, and a
        // runtime change is a follow-up (CONFIG.md "accepted and echoed").
        ParamSpec {
            name: "list-max-listpack-size",
            kind: SetKind::AcceptedNoOp,
        },
        // The hash listpack->hashtable thresholds (PR-6, #40): entry-count cap (512)
        // and per-element byte cap (64). Recognized + echoed for compatibility; the
        // store reads its own resolved defaults, and a runtime change is a follow-up
        // (CONFIG.md "accepted and echoed").
        ParamSpec {
            name: "hash-max-listpack-entries",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "hash-max-listpack-value",
            kind: SetKind::AcceptedNoOp,
        },
        // The set intset->listpack->hashtable thresholds (PR-7, #40): the all-integer
        // intset entry cap (512), the listpack entry cap (128), and the listpack
        // per-member byte cap (64). Recognized + echoed for compatibility; the store
        // reads its own resolved defaults, and a runtime change is a follow-up
        // (CONFIG.md "accepted and echoed").
        ParamSpec {
            name: "set-max-intset-entries",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "set-max-listpack-entries",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "set-max-listpack-value",
            kind: SetKind::AcceptedNoOp,
        },
        // The zset listpack->skiplist thresholds (PR-8, #40): the listpack entry cap
        // (128) and the listpack per-member byte cap (64). Recognized + echoed for
        // compatibility; the store reads its own resolved defaults, and a runtime change
        // is a follow-up (CONFIG.md "accepted and echoed").
        ParamSpec {
            name: "zset-max-listpack-entries",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "zset-max-listpack-value",
            kind: SetKind::AcceptedNoOp,
        },
        // bind/port are MODIFIABLE_CONFIG in Redis (accepted at runtime), but IronCache
        // reports them restart-required as a DELIBERATE DIVERGENCE: the thread-per-core
        // boot binds the listening sockets once at startup and cannot re-bind / re-port
        // live, so a runtime set is rejected rather than silently lying. See the
        // `SetKind::RestartRequired` doc and docs/design/CONFIG.md. (databases/io-threads
        // below are genuinely IMMUTABLE in both Redis and IronCache.)
        ParamSpec {
            name: "bind",
            kind: SetKind::RestartRequired,
        },
        ParamSpec {
            name: "port",
            kind: SetKind::RestartRequired,
        },
        ParamSpec {
            name: "databases",
            kind: SetKind::RestartRequired,
        },
        // The shard/core count: reported under the Redis-recognized `io-threads` name
        // (IronCache's thread-per-core analog) AND the native `shards` name, both
        // restart-required (CONFIG.md "the shard/core count cannot change at runtime").
        ParamSpec {
            name: "io-threads",
            kind: SetKind::RestartRequired,
        },
        ParamSpec {
            name: "shards",
            kind: SetKind::RestartRequired,
        },
    ]
}

/// Look up a registered param by name (case-insensitive, like Redis). `None` means
/// the param is not recognized (a `CONFIG GET` omits it; a `CONFIG SET` errors).
#[must_use]
pub fn lookup(name: &str) -> Option<&'static ParamSpec> {
    let lower = name.to_ascii_lowercase();
    param_specs().iter().find(|s| s.name == lower)
}

/// The current EFFECTIVE value of `name` as a display string, resolving the runtime
/// overlay (highest precedence) over the boot config. `None` if the param is not
/// registered. This is the value `CONFIG GET` returns.
///
/// The runtime-settable params (`maxmemory`/`maxmemory-policy`/`requirepass`) read the
/// [`RuntimeConfig`] overlay so a prior `CONFIG SET` is reflected; the rest read the
/// boot [`Config`]. The accepted no-ops return a fixed Redis-recognized value.
#[must_use]
pub fn effective_value(name: &str, runtime: &RuntimeConfig, boot: &Config) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let value = match lower.as_str() {
        // Runtime-settable: read the overlay (a CONFIG SET wins over the boot value).
        "maxmemory" => runtime.maxmemory().to_string(),
        "maxmemory-policy" => runtime.policy_name(),
        // SECURITY DIVERGENCE (#65): `CONFIG GET requirepass` returns the stored SHA-256
        // HEX digest, NOT the plaintext. Redis echoes the plaintext here; IronCache
        // deliberately does not, because retaining the plaintext for CONFIG GET would
        // defeat the at-rest hardening (the password is stored as a digest, AUTH.md /
        // threat-model #142). This is low-risk: only an AUTHENTICATED client reaches
        // CONFIG GET (it is NOAUTH-gated) and it already knows the password, so exposing
        // the digest leaks nothing it could not compute itself. An unset requirepass
        // reports the empty string (Redis parity for unset, NOT nil). NOTE: the value a
        // client reads here is a hash and is NOT meant to be re-`SET` (CONFIG SET always
        // treats its value as plaintext; the ACL `#<hash>` pre-hashed form is #106).
        "requirepass" => runtime.requirepass().unwrap_or_default(),
        // The connection / output-buffer safety ceilings (PROD-SAFETY #3/#5): read the overlay so a
        // `CONFIG SET` is reflected. `maxclients` is a plain count; `output-buffer-limit` is a byte
        // count (reported as bytes, the form CONFIG SET accepts back).
        "maxclients" => runtime.maxclients().to_string(),
        "output-buffer-limit" => runtime.output_buffer_limit().to_string(),
        // The SLOWLOG knobs (PROD-7): read the overlay so a `CONFIG SET slowlog-*` is reflected.
        "slowlog-log-slower-than" => runtime.slowlog_log_slower_than().to_string(),
        "slowlog-max-len" => runtime.slowlog_max_len().to_string(),
        // Keyspace notifications (PROD-8): render the live overlay flags back to the canonical Redis
        // flag string (the empty string when disabled, the default).
        "notify-keyspace-events" => runtime.notify_flags().render(),
        // Accepted no-ops: fixed Redis-recognized defaults under the cache build.
        // `maxmemory-samples` defaults to 5 in Redis.
        "maxmemory-samples" => "5".to_owned(),
        // `save` reports the REAL runtime save policy (#58 durability footgun fix): the configured
        // interval/min-changes rendered as a Redis `save` point, or the empty string when the
        // periodic save is OFF -- so an operator can see whether durability is actually on.
        "save" => {
            let (interval, changes) = runtime.save_policy();
            crate::render_save_points(interval, changes)
        }
        // `appendonly` is always `no`: IronCache has no AOF (it persists via snapshots).
        "appendonly" => "no".to_owned(),
        // The list listpack->quicklist threshold: echo the Redis `-2` default
        // spelling ("8 KB per node"); the store works in the resolved byte budget.
        "list-max-listpack-size" => crate::LIST_MAX_LISTPACK_SIZE_REDIS_DEFAULT.to_owned(),
        // The hash listpack->hashtable thresholds: echo the pinned defaults (512
        // entries, 64 bytes per element); the store reads these resolved defaults.
        "hash-max-listpack-entries" => crate::DEFAULT_HASH_MAX_LISTPACK_ENTRIES.to_string(),
        "hash-max-listpack-value" => crate::DEFAULT_HASH_MAX_LISTPACK_VALUE.to_string(),
        // The set encoding-ladder thresholds: echo the pinned defaults (intset 512,
        // listpack 128 entries, 64 bytes per member); the store reads these resolved
        // defaults.
        "set-max-intset-entries" => crate::DEFAULT_SET_MAX_INTSET_ENTRIES.to_string(),
        "set-max-listpack-entries" => crate::DEFAULT_SET_MAX_LISTPACK_ENTRIES.to_string(),
        "set-max-listpack-value" => crate::DEFAULT_SET_MAX_LISTPACK_VALUE.to_string(),
        // The zset listpack->skiplist thresholds: echo the pinned defaults (128 entries,
        // 64 bytes per member); the store reads these resolved defaults (PR-8).
        "zset-max-listpack-entries" => crate::DEFAULT_ZSET_MAX_LISTPACK_ENTRIES.to_string(),
        "zset-max-listpack-value" => crate::DEFAULT_ZSET_MAX_LISTPACK_VALUE.to_string(),
        // Restart-required: read the boot config (these never change at runtime).
        "bind" => boot.bind.to_string(),
        "port" => boot.port.to_string(),
        "databases" => boot.databases.to_string(),
        "io-threads" | "shards" => boot.shards.to_string(),
        _ => return None,
    };
    Some(value)
}

/// Apply a `CONFIG SET name value` to the runtime overlay, returning the [`SetOutcome`].
/// The command layer maps the outcome to `+OK` or the appropriate canonical error.
///
/// Validation mirrors boot validation: `maxmemory` goes through [`parse_human_size`]
/// (a bad size is [`SetOutcome::InvalidValue`]); `maxmemory-policy` must be one of the
/// eight Redis names; `requirepass` accepts any string (empty disables auth). A
/// restart-required param returns [`SetOutcome::RestartRequired`]; an accepted no-op
/// returns [`SetOutcome::Applied`] without storing anything.
pub fn apply_set(name: &str, value: &str, runtime: &RuntimeConfig) -> SetOutcome {
    let Some(spec) = lookup(name) else {
        return SetOutcome::UnknownParam;
    };
    match spec.kind {
        SetKind::RestartRequired => SetOutcome::RestartRequired,
        // Accepted but inert: ack without changing anything (CONFIG.md no-op params).
        SetKind::AcceptedNoOp => SetOutcome::Applied,
        SetKind::Runtime => apply_runtime_set(spec.name, value, runtime),
        // Recognized but unsupported (currently only `appendonly`): VALUE-SENSITIVE. Turning the
        // feature OFF is a no-op-OK (the feature is already off / has nothing to disable), so
        // `CONFIG SET appendonly no` MUST reply +OK -- a client / ops tool that defensively sets
        // `appendonly no` at startup expects success (Redis accepts it). Turning the feature ON
        // (`appendonly yes`) is REFUSED with an explicit error rather than silently accepted (the
        // false-durability footgun fix #58: IronCache has no AOF, so accepting `yes` would let an
        // operator believe AOF durability is on when it is not). A non-boolean value is rejected as
        // an invalid value (Redis: a bad boolean is "argument must be 'yes' or 'no'").
        SetKind::Unsupported => apply_unsupported_set(spec.name, value),
    }
}

/// Apply a `CONFIG SET` to a recognized-but-unsupported param. `appendonly` (the only one):
/// `no`/`0`/`false` -> [`SetOutcome::Applied`] (a no-op-OK: the feature is already off);
/// `yes`/`1`/`true` -> [`SetOutcome::Unsupported`] (refuse the false-durability claim);
/// anything else -> [`SetOutcome::InvalidValue`] (a malformed boolean). The match is
/// case-insensitive (Redis parses the boolean case-insensitively).
fn apply_unsupported_set(name: &str, value: &str) -> SetOutcome {
    match name {
        "appendonly" => match value.to_ascii_lowercase().as_str() {
            // OFF is a no-op-OK (there is nothing to disable; Redis accepts it).
            "no" | "0" | "false" => SetOutcome::Applied,
            // ON is refused: this build has no AOF (the #58 durability footgun fix).
            "yes" | "1" | "true" => SetOutcome::Unsupported(unsupported_reason(name)),
            // A non-boolean value is a malformed argument (Redis: "argument must be 'yes' or 'no'").
            _ => SetOutcome::InvalidValue("argument must be 'yes' or 'no'".to_owned()),
        },
        // Defensive: any future Unsupported param must add its own value handling.
        other => SetOutcome::Unsupported(unsupported_reason(other)),
    }
}

/// The operator-facing reason a recognized-but-unsupported param is refused. Currently only
/// `appendonly` (no AOF in this build): point the operator at the snapshot durability path so the
/// refusal is actionable, not just a flat "no".
fn unsupported_reason(name: &str) -> String {
    match name {
        "appendonly" => "AOF/appendonly is not supported; this build persists via snapshots, \
                          see save / SAVE/BGSAVE"
            .to_owned(),
        // Defensive: any future Unsupported param must add its own reason.
        other => format!("'{other}' is not supported by this build"),
    }
}

/// Apply a runtime-settable param to the overlay. Split out so the per-param
/// validation + overlay mutation lives in one place.
fn apply_runtime_set(name: &str, value: &str, runtime: &RuntimeConfig) -> SetOutcome {
    match name {
        "maxmemory" => match parse_human_size(value) {
            Ok(bytes) => {
                runtime.set_maxmemory(bytes);
                SetOutcome::Applied
            }
            Err(e) => SetOutcome::InvalidValue(e.to_string()),
        },
        "maxmemory-policy" => {
            let lower = value.to_ascii_lowercase();
            if MAXMEMORY_POLICY_NAMES.contains(&lower.as_str()) {
                // Store the lowercased canonical spelling (Redis accepts any case and
                // reports the canonical name), bumping the generation so shards swap.
                runtime.set_policy_name(&lower);
                SetOutcome::Applied
            } else {
                SetOutcome::InvalidValue(format!(
                    "'{lower}' is not a valid maxmemory-policy (expected one of: {})",
                    MAXMEMORY_POLICY_NAMES.join(", ")
                ))
            }
        }
        "requirepass" => {
            // `value` is ALWAYS a PLAINTEXT password (Redis requirepass semantics);
            // set_requirepass hashes it to SHA-256 before storing (#65). The ACL
            // `#<hash>` pre-hashed syntax is #106 (later), so a digest read back via
            // CONFIG GET is not meant to be re-SET here. An empty value disables auth.
            runtime.set_requirepass(value);
            SetOutcome::Applied
        }
        // The simultaneous-connection ceiling (PROD-SAFETY #3): a plain non-negative integer
        // count; `0` disables the cap. A malformed value is an invalid value (never a silent 0).
        "maxclients" => match value.parse::<u64>() {
            Ok(n) => {
                runtime.set_maxclients(n);
                SetOutcome::Applied
            }
            Err(_) => {
                SetOutcome::InvalidValue(format!("'{value}' is not a valid maxclients count"))
            }
        },
        // The per-connection output-buffer hard cap (PROD-SAFETY #5): a byte count accepted as a
        // human size ("256mb") OR a plain integer; `0` disables it. A malformed value is rejected.
        "output-buffer-limit" => match crate::parse_human_size(value) {
            Ok(bytes) => {
                runtime.set_output_buffer_limit(bytes);
                SetOutcome::Applied
            }
            Err(e) => SetOutcome::InvalidValue(e.to_string()),
        },
        "save" => {
            // `CONFIG SET save "<seconds> <changes> [...]"` (#58 durability footgun fix): parse the
            // Redis `save` directive into the live periodic-save policy the saver reads each tick.
            // An empty string disables it; a malformed directive is an invalid value (never a
            // silent accept). The periodic saver picks up the new policy on its next tick.
            match crate::parse_save_points(value) {
                Ok(Some((interval, changes))) => {
                    runtime.set_save_policy(interval, changes);
                    SetOutcome::Applied
                }
                Ok(None) => {
                    runtime.set_save_policy(0, 0);
                    SetOutcome::Applied
                }
                Err(reason) => SetOutcome::InvalidValue(reason),
            }
        }
        // The SLOWLOG threshold (PROD-7): a SIGNED integer microsecond value. `-1` disables the
        // SLOWLOG; `0` logs everything; a positive value is the minimum micros to log. A non-integer
        // is rejected (never a silent default).
        "slowlog-log-slower-than" => match value.parse::<i64>() {
            Ok(micros) => {
                runtime.set_slowlog_log_slower_than(micros);
                SetOutcome::Applied
            }
            Err(_) => {
                SetOutcome::InvalidValue("argument couldn't be parsed into an integer".to_owned())
            }
        },
        // The SLOWLOG max length (PROD-7): a non-negative integer entry cap. A non-integer is
        // rejected.
        "slowlog-max-len" => match value.parse::<u64>() {
            Ok(n) => {
                runtime.set_slowlog_max_len(n);
                SetOutcome::Applied
            }
            Err(_) => {
                SetOutcome::InvalidValue("argument couldn't be parsed into an integer".to_owned())
            }
        },
        // Keyspace notifications (PROD-8): parse the flag string into the live overlay. An
        // unrecognized flag character is rejected (Redis rejects a bad `notify-keyspace-events`);
        // the empty string DISABLES notifications.
        "notify-keyspace-events" => match crate::NotifyFlags::parse(value) {
            Ok(flags) => {
                runtime.set_notify_flags(flags);
                SetOutcome::Applied
            }
            Err(bad) => SetOutcome::InvalidValue(format!(
                "Invalid argument '{bad}' for CONFIG SET 'notify-keyspace-events'"
            )),
        },
        // Defensive: any future Runtime param must add a branch here. An unhandled Runtime name is a
        // programming error, surfaced as an invalid value rather than a silent success.
        other => SetOutcome::InvalidValue(format!("no runtime setter for '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boot() -> Config {
        Config {
            maxmemory: 2048,
            maxmemory_policy: "allkeys-lru".to_owned(),
            requirepass: None,
            ..Config::default()
        }
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(lookup("MaxMemory").is_some());
        assert!(lookup("MAXMEMORY-POLICY").is_some());
        assert!(lookup("nonsense").is_none());
    }

    #[test]
    fn effective_value_reads_overlay_then_boot() {
        let cfg = boot();
        let rc = RuntimeConfig::from_config(&cfg);
        // Initially equal to the boot value.
        assert_eq!(
            effective_value("maxmemory", &rc, &cfg).as_deref(),
            Some("2048")
        );
        assert_eq!(
            effective_value("maxmemory-policy", &rc, &cfg).as_deref(),
            Some("allkeys-lru")
        );
        // requirepass unset reports the empty string (Redis parity), not nil.
        assert_eq!(
            effective_value("requirepass", &rc, &cfg).as_deref(),
            Some("")
        );
        // A CONFIG SET makes the overlay win.
        assert_eq!(apply_set("maxmemory", "4096", &rc), SetOutcome::Applied);
        assert_eq!(
            effective_value("maxmemory", &rc, &cfg).as_deref(),
            Some("4096")
        );
        // Restart-required params read the boot config.
        assert_eq!(effective_value("port", &rc, &cfg).as_deref(), Some("6379"));
        assert_eq!(
            effective_value("databases", &rc, &cfg).as_deref(),
            Some("16")
        );
        // The shard count under both names.
        let shards = cfg.shards.to_string();
        assert_eq!(
            effective_value("io-threads", &rc, &cfg).as_deref(),
            Some(shards.as_str())
        );
        assert_eq!(
            effective_value("shards", &rc, &cfg).as_deref(),
            Some(shards.as_str())
        );
        // Accepted no-ops report fixed defaults.
        assert_eq!(
            effective_value("maxmemory-samples", &rc, &cfg).as_deref(),
            Some("5")
        );
        // appendonly is always `no` (no AOF in this build).
        assert_eq!(
            effective_value("appendonly", &rc, &cfg).as_deref(),
            Some("no")
        );
        // `save` reports the REAL runtime save policy (#58 footgun fix): empty when off.
        assert_eq!(effective_value("save", &rc, &cfg).as_deref(), Some(""));
        // The list/hash collection thresholds echo their pinned defaults (PR-5/6).
        assert_eq!(
            effective_value("list-max-listpack-size", &rc, &cfg).as_deref(),
            Some("-2")
        );
        // hash-max-listpack-entries echoes the Redis-correct 512 (NOT 128, which is the
        // ZSET/SET default); verified vs the pinned claim redis-hash-max-listpack-entries-512.
        assert_eq!(
            effective_value("hash-max-listpack-entries", &rc, &cfg).as_deref(),
            Some("512")
        );
        assert_eq!(
            effective_value("hash-max-listpack-value", &rc, &cfg).as_deref(),
            Some("64")
        );
        assert!(lookup("hash-max-listpack-entries").is_some());
        assert!(lookup("hash-max-listpack-value").is_some());
        // The set encoding-ladder thresholds echo their pinned defaults (PR-7):
        // intset 512, listpack 128 entries, 64 bytes per member
        // (redis-set-encodings-thresholds).
        assert_eq!(
            effective_value("set-max-intset-entries", &rc, &cfg).as_deref(),
            Some("512")
        );
        assert_eq!(
            effective_value("set-max-listpack-entries", &rc, &cfg).as_deref(),
            Some("128")
        );
        assert_eq!(
            effective_value("set-max-listpack-value", &rc, &cfg).as_deref(),
            Some("64")
        );
        assert!(lookup("set-max-intset-entries").is_some());
        assert!(lookup("set-max-listpack-entries").is_some());
        assert!(lookup("set-max-listpack-value").is_some());
        // The zset listpack->skiplist thresholds echo their pinned defaults (PR-8):
        // 128 entries, 64 bytes per member (redis-zset-max-listpack-entries-128).
        assert_eq!(
            effective_value("zset-max-listpack-entries", &rc, &cfg).as_deref(),
            Some("128")
        );
        assert_eq!(
            effective_value("zset-max-listpack-value", &rc, &cfg).as_deref(),
            Some("64")
        );
        assert!(lookup("zset-max-listpack-entries").is_some());
        assert!(lookup("zset-max-listpack-value").is_some());
        // Unknown param -> None (CONFIG GET omits it).
        assert!(effective_value("bogus", &rc, &cfg).is_none());
    }

    #[test]
    fn apply_set_validates_and_classifies() {
        let cfg = boot();
        let rc = RuntimeConfig::from_config(&cfg);
        // maxmemory accepts human sizes.
        assert_eq!(apply_set("maxmemory", "100mb", &rc), SetOutcome::Applied);
        assert_eq!(rc.maxmemory(), 100 * 1024 * 1024);
        // A bad size is an invalid value (not a silent 0).
        assert!(matches!(
            apply_set("maxmemory", "1.5gb", &rc),
            SetOutcome::InvalidValue(_)
        ));
        // policy accepts the eight names (case-insensitive), stores lowercase.
        assert_eq!(
            apply_set("maxmemory-policy", "AllKeys-LFU", &rc),
            SetOutcome::Applied
        );
        assert_eq!(rc.policy_name(), "allkeys-lfu");
        // an unknown policy is an invalid value.
        assert!(matches!(
            apply_set("maxmemory-policy", "allkeys-ttl", &rc),
            SetOutcome::InvalidValue(_)
        ));
        // requirepass accepts any plaintext string; empty disables auth.
        assert_eq!(apply_set("requirepass", "pw", &rc), SetOutcome::Applied);
        assert!(rc.requires_auth());
        // SECURITY (#65): CONFIG GET requirepass returns the SHA-256 HEX of the plaintext
        // that was SET, never the plaintext, and never nil.
        let cfg = boot();
        assert_eq!(
            effective_value("requirepass", &rc, &cfg).as_deref(),
            Some(crate::sha256_hex(b"pw").as_str())
        );
        assert_ne!(
            effective_value("requirepass", &rc, &cfg).as_deref(),
            Some("pw")
        );
        assert_eq!(apply_set("requirepass", "", &rc), SetOutcome::Applied);
        assert!(!rc.requires_auth());
        // Unset reports the empty string (Redis parity for unset), not nil.
        assert_eq!(
            effective_value("requirepass", &rc, &cfg).as_deref(),
            Some("")
        );
        // restart-required params report RestartRequired.
        assert_eq!(apply_set("port", "7000", &rc), SetOutcome::RestartRequired);
        assert_eq!(
            apply_set("databases", "8", &rc),
            SetOutcome::RestartRequired
        );
        assert_eq!(apply_set("shards", "4", &rc), SetOutcome::RestartRequired);
        // `maxmemory-samples` is still an accepted no-op (acks without changing anything).
        assert_eq!(
            apply_set("maxmemory-samples", "10", &rc),
            SetOutcome::Applied
        );
        // unknown param.
        assert_eq!(apply_set("bogus", "1", &rc), SetOutcome::UnknownParam);
    }

    /// #58 durability footgun fix: `save` is RUNTIME-SETTABLE -- `apply_set` parses the Redis save
    /// directive into the live policy `effective_value("save")` then reports back, and `""` disables
    /// it. A malformed directive is an invalid value (never a silent accept).
    #[test]
    fn apply_set_save_updates_the_runtime_policy() {
        let cfg = boot();
        let rc = RuntimeConfig::from_config(&cfg);
        // Default off -> empty.
        assert_eq!(effective_value("save", &rc, &cfg).as_deref(), Some(""));
        // SET save "900 1" updates the policy and is reported back.
        assert_eq!(apply_set("save", "900 1", &rc), SetOutcome::Applied);
        assert_eq!(rc.save_policy(), (900, 1));
        assert_eq!(effective_value("save", &rc, &cfg).as_deref(), Some("900 1"));
        // Multiple points collapse to the shortest interval.
        assert_eq!(apply_set("save", "3600 1 60 100", &rc), SetOutcome::Applied);
        assert_eq!(rc.save_policy(), (60, 100));
        // "" disables it.
        assert_eq!(apply_set("save", "", &rc), SetOutcome::Applied);
        assert_eq!(rc.save_policy(), (0, 0));
        assert_eq!(effective_value("save", &rc, &cfg).as_deref(), Some(""));
        // A malformed directive (odd token count) is an invalid value.
        assert!(matches!(
            apply_set("save", "900", &rc),
            SetOutcome::InvalidValue(_)
        ));
        // A zero-second save point is rejected (use "" to disable).
        assert!(matches!(
            apply_set("save", "0 1", &rc),
            SetOutcome::InvalidValue(_)
        ));
    }

    /// #58 durability footgun fix: `appendonly` is UNSUPPORTED -- `CONFIG SET appendonly yes` is
    /// refused (no AOF in this build), not silently accepted.
    #[test]
    fn apply_set_appendonly_is_unsupported() {
        let cfg = boot();
        let rc = RuntimeConfig::from_config(&cfg);
        match apply_set("appendonly", "yes", &rc) {
            SetOutcome::Unsupported(reason) => {
                assert!(reason.contains("not supported"), "got {reason}");
                assert!(reason.contains("snapshot"), "got {reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
