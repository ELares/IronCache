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
        ParamSpec {
            name: "maxmemory-samples",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "save",
            kind: SetKind::AcceptedNoOp,
        },
        ParamSpec {
            name: "appendonly",
            kind: SetKind::AcceptedNoOp,
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
        // Redis reports an unset requirepass as the empty string (NOT nil).
        "requirepass" => runtime.requirepass().unwrap_or_default(),
        // Accepted no-ops: fixed Redis-recognized defaults under the cache build.
        // `maxmemory-samples` defaults to 5 in Redis; save/appendonly default to off.
        "maxmemory-samples" => "5".to_owned(),
        "save" => String::new(),
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
            runtime.set_requirepass(value);
            SetOutcome::Applied
        }
        // Defensive: the registry only marks these three Runtime; any future Runtime
        // param must add a branch here. An unhandled Runtime name is a programming
        // error, surfaced as an invalid value rather than a silent success.
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
        assert_eq!(
            effective_value("appendonly", &rc, &cfg).as_deref(),
            Some("no")
        );
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
        // requirepass accepts any string; empty disables auth.
        assert_eq!(apply_set("requirepass", "pw", &rc), SetOutcome::Applied);
        assert!(rc.requires_auth());
        assert_eq!(apply_set("requirepass", "", &rc), SetOutcome::Applied);
        assert!(!rc.requires_auth());
        // restart-required params report RestartRequired.
        assert_eq!(apply_set("port", "7000", &rc), SetOutcome::RestartRequired);
        assert_eq!(
            apply_set("databases", "8", &rc),
            SetOutcome::RestartRequired
        );
        assert_eq!(apply_set("shards", "4", &rc), SetOutcome::RestartRequired);
        // accepted no-ops ack without changing anything.
        assert_eq!(apply_set("save", "900 1", &rc), SetOutcome::Applied);
        assert_eq!(
            apply_set("maxmemory-samples", "10", &rc),
            SetOutcome::Applied
        );
        // unknown param.
        assert_eq!(apply_set("bogus", "1", &rc), SetOutcome::UnknownParam);
    }
}
