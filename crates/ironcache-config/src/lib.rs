// SPDX-License-Identifier: MIT OR Apache-2.0
//! Layered configuration for IronCache (CONFIG.md, #85).
//!
//! The effective value of each key is resolved across ordered layers, highest
//! precedence first (CONFIG.md "sources and precedence"):
//!
//! ```text
//! CLI flags  >  environment variables  >  TOML file  >  built-in defaults
//! ```
//!
//! (The runtime `CONFIG SET` layer that sits above CLI flags is part of the wire
//! command surface and lands with the `CONFIG` command in a later PR; PR-1
//! implements the four startup layers.)
//!
//! Each layer is an [`ConfigOverlay`] of optional fields; [`Config::resolve`]
//! folds them defaults-first so a higher layer that sets a key wins. Human sizes
//! ("512mb") are parsed by [`parse_human_size`].

use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};
use thiserror::Error;

/// The default RESP port. Redis/Valkey use 6379; IronCache keeps it for drop-in
/// compatibility (CLI_BINARY.md leaves the exact port open but defaults to parity).
pub const DEFAULT_PORT: u16 = 6379;

/// The default shard count: the host's available parallelism via
/// [`std::thread::available_parallelism`] (CONFIG.md), which honors cgroup CPU
/// quotas (unlike the `num_cpus` crate). Never zero (a degenerate host reports at
/// least one).
#[must_use]
pub fn default_shards() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Errors from loading or resolving configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The TOML file could not be parsed.
    #[error("config file parse error: {0}")]
    Toml(#[from] toml::de::Error),
    /// An I/O error reading the config file.
    #[error("config file read error: {0}")]
    Io(String),
    /// A human-size string ("512mb") was malformed.
    #[error("invalid size value '{0}': {1}")]
    Size(String, String),
    /// A field held an out-of-range or otherwise invalid value.
    #[error("invalid config value for {field}: {reason}")]
    Invalid {
        /// The offending field name.
        field: &'static str,
        /// Why it was rejected.
        reason: String,
    },
}

/// The fully-resolved, effective configuration the server boots from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Address to bind the RESP listener.
    pub bind: IpAddr,
    /// TCP port for the RESP listener.
    pub port: u16,
    /// Number of shards / per-core runtimes. Defaults to available parallelism.
    pub shards: usize,
    /// Number of logical databases (`SELECT 0..databases-1`). Redis default 16.
    pub databases: u32,
    /// The default protocol for a new connection before `HELLO 3` (always RESP2
    /// per PROTOCOL.md; exposed for completeness/testing).
    pub default_resp3: bool,
    /// Memory ceiling in bytes. `0` means unlimited (PR-1 does not enforce it;
    /// the value is parsed and surfaced for `config`/INFO).
    pub maxmemory: u64,
    /// Optional `requirepass` password. `None` means auth is not required.
    pub requirepass: Option<String>,
    /// Idle timeout in seconds; `0` disables idle disconnection (Redis default 0,
    /// CONNECTION_LIFECYCLE.md).
    pub timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        // Built-in safe defaults (the lowest-precedence layer; CONFIG.md /
        // ADR-0007 cache-mode posture). Bind to loopback by default so an
        // unconfigured boot is not exposed on all interfaces.
        Config {
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            // Default to available parallelism via std (which honors cgroup CPU
            // quotas, unlike num_cpus). Never zero. Mirrors
            // `ironcache_runtime::available_shards` without taking a dep on the
            // runtime crate (and thus tokio) here.
            shards: default_shards(),
            databases: 16,
            default_resp3: false,
            maxmemory: 0,
            requirepass: None,
            timeout_secs: 0,
        }
    }
}

impl Config {
    /// Resolve the effective config by folding `overlays` over the defaults,
    /// lowest-precedence first. The caller passes overlays in precedence order:
    /// `[toml, env, cli]` (later overrides earlier).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Size`] if any overlay carries a malformed or
    /// out-of-range `maxmemory` (e.g. `garbage`, `99999999999gb`, `1.5gb`). A bad
    /// ceiling HARD-FAILS here rather than silently resolving to 0 = unlimited,
    /// which would violate the honest-ceiling invariant (#3).
    pub fn resolve(overlays: &[ConfigOverlay]) -> Result<Config, ConfigError> {
        let mut cfg = Config::default();
        for o in overlays {
            o.apply_to(&mut cfg)?;
        }
        Ok(cfg)
    }

    /// Validate cross-field invariants after resolution.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.shards == 0 {
            return Err(ConfigError::Invalid {
                field: "shards",
                reason: "must be at least 1".to_owned(),
            });
        }
        if self.databases == 0 {
            return Err(ConfigError::Invalid {
                field: "databases",
                reason: "must be at least 1".to_owned(),
            });
        }
        Ok(())
    }
}

/// A single layer of optional overrides. The TOML file deserializes directly into
/// this; the env and CLI layers construct it field by field. A `None` field means
/// "this layer does not set this key" and the lower layer shows through.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverlay {
    /// Bind address (string form, parsed on apply).
    pub bind: Option<IpAddr>,
    /// Port.
    pub port: Option<u16>,
    /// Shard count.
    pub shards: Option<usize>,
    /// Database count.
    pub databases: Option<u32>,
    /// Whether new connections default to RESP3.
    pub default_resp3: Option<bool>,
    /// Memory ceiling as a human size string ("512mb", "1gb", "0").
    pub maxmemory: Option<String>,
    /// `requirepass` password.
    pub requirepass: Option<String>,
    /// Idle timeout in seconds.
    pub timeout: Option<u64>,
}

impl ConfigOverlay {
    /// Parse a TOML document into an overlay.
    pub fn from_toml_str(s: &str) -> Result<ConfigOverlay, ConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Load an overlay from a TOML file path. A missing file yields an empty
    /// overlay (an absent config file is allowed, CONFIG.md / Redis parity).
    pub fn from_toml_file(path: &std::path::Path) -> Result<ConfigOverlay, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(s) => ConfigOverlay::from_toml_str(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConfigOverlay::default()),
            Err(e) => Err(ConfigError::Io(e.to_string())),
        }
    }

    /// Build an overlay from `IRONCACHE_*` environment variables. Unset variables
    /// leave their field `None`. Size/number parse errors are returned.
    pub fn from_env() -> Result<ConfigOverlay, ConfigError> {
        let mut o = ConfigOverlay::default();
        if let Ok(v) = std::env::var("IRONCACHE_BIND") {
            o.bind = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "bind",
                reason: format!("not an IP address: {v}"),
            })?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_PORT") {
            o.port = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "port",
                reason: format!("not a port: {v}"),
            })?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_SHARDS") {
            o.shards = Some(v.parse().map_err(|_| ConfigError::Invalid {
                field: "shards",
                reason: format!("not a number: {v}"),
            })?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_MAXMEMORY") {
            o.maxmemory = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_REQUIREPASS") {
            o.requirepass = Some(v);
        }
        Ok(o)
    }

    /// Apply this overlay's set fields onto `cfg`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Size`] if this overlay's `maxmemory` is malformed or
    /// out of range. A bad ceiling propagates so the binary hard-fails at boot
    /// rather than silently going unlimited (honest-ceiling invariant #3).
    fn apply_to(&self, cfg: &mut Config) -> Result<(), ConfigError> {
        if let Some(v) = self.bind {
            cfg.bind = v;
        }
        if let Some(v) = self.port {
            cfg.port = v;
        }
        if let Some(v) = self.shards {
            cfg.shards = v;
        }
        if let Some(v) = self.databases {
            cfg.databases = v;
        }
        if let Some(v) = self.default_resp3 {
            cfg.default_resp3 = v;
        }
        if let Some(ref v) = self.maxmemory {
            // Parse the human size here and PROPAGATE any error: a malformed or
            // overflowing maxmemory must hard-fail boot, not silently become 0
            // (unlimited). Integer math, overflow-checked (see parse_human_size).
            cfg.maxmemory = parse_human_size(v)?;
        }
        if let Some(ref v) = self.requirepass {
            cfg.requirepass = Some(v.clone());
        }
        if let Some(v) = self.timeout {
            cfg.timeout_secs = v;
        }
        Ok(())
    }
}

/// Parse a human-readable size into bytes, accepting Redis-style suffixes:
/// `b`, `k`/`kb`, `m`/`mb`, `g`/`gb` (and uppercase). Bare numbers are bytes.
/// `k`/`m`/`g` are 1000-based and `kb`/`mb`/`gb` are 1024-based, matching Redis's
/// `memtoull` convention. `0` parses to `0` (unlimited).
///
/// The numeric prefix is parsed as a `u64` and multiplied by the unit with
/// `checked_mul`, so all arithmetic is integer and overflow is a hard error
/// (never a silent wrap). FRACTIONAL inputs are REJECTED: `1.5gb` returns a
/// [`ConfigError::Size`] rather than truncating, because a cache ceiling must be
/// an exact byte count and silent truncation hides operator intent. A leading `+`
/// is rejected too (Redis sizes are plain non-negative integers).
pub fn parse_human_size(s: &str) -> Result<u64, ConfigError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(ConfigError::Size(s.to_owned(), "empty".to_owned()));
    }
    // Numeric prefix is ASCII digits only (no '.', '+', '-'): any of those makes
    // the input either fractional, signed, or malformed, all of which we reject.
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num_part, unit_part) = t.split_at(split);
    if num_part.is_empty() {
        return Err(ConfigError::Size(
            s.to_owned(),
            "missing numeric value".to_owned(),
        ));
    }
    let unit = unit_part.trim().to_ascii_lowercase();
    let mult: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" => 1_000,
        "kb" => 1_024,
        "m" => 1_000_000,
        "mb" => 1_024 * 1_024,
        "g" => 1_000_000_000,
        "gb" => 1_024 * 1_024 * 1_024,
        other => {
            return Err(ConfigError::Size(
                s.to_owned(),
                format!("unknown unit '{other}'"),
            ));
        }
    };
    // Integer parse of the numeric prefix. A '.', '+', or '-' would have ended the
    // digit run above and shown up in `unit`, producing an "unknown unit" error;
    // an explicit check keeps the message precise for the common fractional case.
    if num_part.len() != t.len() && unit_part.starts_with('.') {
        return Err(ConfigError::Size(
            s.to_owned(),
            "fractional sizes are not allowed".to_owned(),
        ));
    }
    let value: u64 = num_part
        .parse()
        .map_err(|_| ConfigError::Size(s.to_owned(), "not an integer".to_owned()))?;
    value.checked_mul(mult).ok_or_else(|| {
        ConfigError::Size(s.to_owned(), "too large (overflows u64 bytes)".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = Config::default();
        assert_eq!(c.port, 6379);
        assert!(c.shards >= 1);
        assert_eq!(c.databases, 16);
        assert_eq!(c.maxmemory, 0);
        assert!(c.requirepass.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn precedence_cli_over_env_over_file() {
        let file = ConfigOverlay {
            port: Some(1111),
            shards: Some(2),
            ..Default::default()
        };
        let env = ConfigOverlay {
            port: Some(2222),
            ..Default::default()
        };
        let cli = ConfigOverlay {
            port: Some(3333),
            ..Default::default()
        };
        let cfg = Config::resolve(&[file, env, cli]).unwrap();
        // CLI wins on port.
        assert_eq!(cfg.port, 3333);
        // shards only set by file -> shows through.
        assert_eq!(cfg.shards, 2);
    }

    #[test]
    fn toml_parse_roundtrip() {
        let toml_src = r#"
            port = 7000
            shards = 4
            maxmemory = "256mb"
            requirepass = "secret"
        "#;
        let o = ConfigOverlay::from_toml_str(toml_src).unwrap();
        let cfg = Config::resolve(&[o]).unwrap();
        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.shards, 4);
        assert_eq!(cfg.maxmemory, 256 * 1024 * 1024);
        assert_eq!(cfg.requirepass.as_deref(), Some("secret"));
    }

    #[test]
    fn maxmemory_bad_value_hard_fails_resolution() {
        // A malformed/overflowing/fractional maxmemory must error out of resolve,
        // NOT silently resolve to 0 (unlimited).
        for bad in ["garbage", "99999999999gb", "1.7b", "1.5gb", "12xb", "-5mb"] {
            let o = ConfigOverlay {
                maxmemory: Some(bad.to_owned()),
                ..Default::default()
            };
            let res = Config::resolve(&[o]);
            assert!(
                matches!(res, Err(ConfigError::Size(_, _))),
                "expected Size error for maxmemory {bad:?}, got {res:?}"
            );
        }
    }

    #[test]
    fn maxmemory_good_value_resolves() {
        let o = ConfigOverlay {
            maxmemory: Some("512mb".to_owned()),
            ..Default::default()
        };
        let cfg = Config::resolve(&[o]).unwrap();
        assert_eq!(cfg.maxmemory, 512 * 1024 * 1024);
    }

    #[test]
    fn toml_rejects_unknown_field() {
        let res = ConfigOverlay::from_toml_str("nonsense = 1");
        assert!(res.is_err());
    }

    #[test]
    fn human_size_units() {
        assert_eq!(parse_human_size("0").unwrap(), 0);
        assert_eq!(parse_human_size("512").unwrap(), 512);
        assert_eq!(parse_human_size("512b").unwrap(), 512);
        assert_eq!(parse_human_size("1k").unwrap(), 1_000);
        assert_eq!(parse_human_size("1kb").unwrap(), 1_024);
        assert_eq!(parse_human_size("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_human_size("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_human_size("1G").unwrap(), 1_000_000_000);
        // whitespace tolerated.
        assert_eq!(parse_human_size("  64mb ").unwrap(), 64 * 1024 * 1024);
    }

    #[test]
    fn human_size_rejects_garbage() {
        assert!(parse_human_size("").is_err());
        assert!(parse_human_size("abc").is_err());
        assert!(parse_human_size("12xb").is_err());
        assert!(parse_human_size("-5mb").is_err());
        // Fractional inputs are rejected (no silent truncation).
        assert!(parse_human_size("1.5gb").is_err());
        assert!(parse_human_size("1.7b").is_err());
        // Overflow is a hard error, not a silent wrap.
        assert!(parse_human_size("99999999999gb").is_err());
        assert!(parse_human_size("18446744073709551616").is_err()); // u64::MAX + 1
    }

    #[test]
    fn validate_rejects_zero_shards() {
        let c = Config {
            shards: 0,
            ..Config::default()
        };
        assert!(c.validate().is_err());
    }
}
