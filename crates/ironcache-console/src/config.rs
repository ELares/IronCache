// SPDX-License-Identifier: MIT OR Apache-2.0
//! Layered console configuration (issue #353): defaults -> TOML file ->
//! `IRONCACHE_CONSOLE_*` env -> CLI flags. Hand-rolled serde + toml, mirroring
//! the engine's `ironcache-config` pattern (no figment / config crate).
//!
//! The console's config is intentionally about WHERE to look and HOW to connect,
//! never the node secret inline: `node_password_file` is a path to a secret
//! (read at connect time in #355), so the plaintext never lands in the config
//! file, env, or the `config` dump.

use std::path::{Path, PathBuf};

/// Default HTTP listen address for the console (its API/UI/`/metrics` surface).
/// LOOPBACK by default: the console is an admin/monitoring plane (it will hang
/// the `/api/*` surface and the UI off this listener and authenticates to nodes
/// with an ACL user), so it must NOT be world-reachable unless deliberately
/// exposed. Operators set `http_addr` explicitly (behind a VPN-locked LB, per
/// the security plan #369) to expose it; a wildcard bind is warned about at boot.
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:9180";
/// Default node-poll interval, used once polling lands in #355.
const DEFAULT_POLL_INTERVAL_SECS: u64 = 10;
/// Default TCP/TLS connect timeout to a node, in seconds (#355). A node that
/// accepts the SYN but never finishes the handshake must not pin the poll loop.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
/// Default per-operation (write + read one reply) timeout to a node, in seconds
/// (#355). A node that accepts a command but never replies must not hang the
/// poller: this is the bound a missing read timeout once lacked in production.
const DEFAULT_OP_TIMEOUT_SECS: u64 = 5;
/// Default log level.
const DEFAULT_LOG_LEVEL: &str = "info";

/// A config error, surfaced as a clean boot failure rather than a panic.
#[derive(Debug, thiserror::Error)]
pub enum ConsoleConfigError {
    /// The TOML file did not parse.
    #[error("parsing console config TOML: {0}")]
    Toml(#[from] toml::de::Error),
    /// The config file could not be read (a present-but-unreadable file).
    #[error("reading console config: {0}")]
    Io(String),
    /// A field held an unparseable / out-of-range value.
    #[error("invalid console config field '{field}': {reason}")]
    Invalid { field: &'static str, reason: String },
}

/// The merge unit: every field optional, so a layer only overrides what it sets.
/// Layers are folded over the defaults in [`ConsoleConfig::resolve`].
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsoleConfigOverlay {
    /// HTTP listen address (`host:port`) for the console's own surface.
    pub http_addr: Option<String>,
    /// Seed IronCache node addresses (`host:port`) to discover the deployment.
    pub seeds: Option<Vec<String>>,
    /// Base URL of the Prometheus the console queries for history (#356).
    pub prometheus_url: Option<String>,
    /// ACL user the console authenticates to nodes as (least-privilege, #367).
    pub node_user: Option<String>,
    /// Path to a file holding the node password (a secret reference, never the
    /// secret inline). Read at connect time.
    pub node_password_file: Option<PathBuf>,
    /// Connect to nodes over TLS (#355). The engine supports server-auth TLS.
    pub node_tls: Option<bool>,
    /// CA bundle (PEM) to verify node TLS certificates.
    pub node_tls_ca: Option<PathBuf>,
    /// Accept ANY node TLS certificate without verifying it against a CA. Default
    /// FALSE: with `node_tls` on and no CA the dial is REJECTED, so the operator
    /// must opt in EXPLICITLY to run encrypted-but-unverified (which is exposed to
    /// an active MITM). Never silently enabled.
    pub node_tls_insecure_skip_verify: Option<bool>,
    /// Node poll interval in seconds (#355).
    pub poll_interval_secs: Option<u64>,
    /// TCP/TLS connect timeout to a node, in seconds (#355).
    pub connect_timeout_secs: Option<u64>,
    /// Per-operation (write + read one reply) timeout to a node, in seconds
    /// (#355). The hard bound that prevents a never-replying node from hanging
    /// the poller.
    pub op_timeout_secs: Option<u64>,
    /// Log level (the CLI `--log-level` is the usual source).
    pub log_level: Option<String>,
    /// Console API read token (#360). Grants the OPEN + PRIVILEGED_READ tiers via
    /// `Authorization: Bearer <token>`. A secret; never logged or shown in the
    /// `config` dump. Configuring it puts the console into ENFORCE mode.
    pub read_token: Option<String>,
    /// Console API admin token (#360). Grants every tier (OPEN + PRIVILEGED_READ +
    /// ADMIN). A secret; never logged or shown in the `config` dump. Configuring it
    /// puts the console into ENFORCE mode.
    pub admin_token: Option<String>,
}

impl ConsoleConfigOverlay {
    /// Parse an overlay from a TOML string.
    pub fn from_toml_str(s: &str) -> Result<Self, ConsoleConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Parse an overlay from a TOML file. A MISSING file is allowed and yields
    /// an empty overlay (defaults plus the other layers still apply); a present
    /// but unreadable file is an error.
    pub fn from_toml_file(path: &Path) -> Result<Self, ConsoleConfigError> {
        match std::fs::read_to_string(path) {
            Ok(s) => Self::from_toml_str(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(ConsoleConfigError::Io(format!("{}: {e}", path.display()))),
        }
    }

    /// Build an overlay from the `IRONCACHE_CONSOLE_*` environment.
    pub fn from_env() -> Result<Self, ConsoleConfigError> {
        let mut overlay = Self::default();
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_HTTP_ADDR") {
            overlay.http_addr = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_SEEDS") {
            overlay.seeds = Some(parse_seed_list(&v));
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_PROMETHEUS_URL") {
            overlay.prometheus_url = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_NODE_USER") {
            overlay.node_user = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_NODE_PASSWORD_FILE") {
            overlay.node_password_file = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_NODE_TLS") {
            overlay.node_tls = Some(parse_bool("IRONCACHE_CONSOLE_NODE_TLS", &v)?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_NODE_TLS_CA") {
            overlay.node_tls_ca = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_NODE_TLS_INSECURE_SKIP_VERIFY") {
            overlay.node_tls_insecure_skip_verify = Some(parse_bool(
                "IRONCACHE_CONSOLE_NODE_TLS_INSECURE_SKIP_VERIFY",
                &v,
            )?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_POLL_INTERVAL_SECS") {
            let n = v
                .trim()
                .parse::<u64>()
                .map_err(|e| ConsoleConfigError::Invalid {
                    field: "poll_interval_secs",
                    reason: format!("not a valid non-negative integer: {e}"),
                })?;
            overlay.poll_interval_secs = Some(n);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_CONNECT_TIMEOUT_SECS") {
            overlay.connect_timeout_secs = Some(parse_u64("connect_timeout_secs", &v)?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_OP_TIMEOUT_SECS") {
            overlay.op_timeout_secs = Some(parse_u64("op_timeout_secs", &v)?);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_LOG_LEVEL") {
            overlay.log_level = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_READ_TOKEN") {
            overlay.read_token = Some(v);
        }
        if let Ok(v) = std::env::var("IRONCACHE_CONSOLE_ADMIN_TOKEN") {
            overlay.admin_token = Some(v);
        }
        Ok(overlay)
    }
}

/// The resolved, effective console configuration.
#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub http_addr: String,
    pub seeds: Vec<String>,
    pub prometheus_url: Option<String>,
    pub node_user: Option<String>,
    pub node_password_file: Option<PathBuf>,
    pub node_tls: bool,
    pub node_tls_ca: Option<PathBuf>,
    pub node_tls_insecure_skip_verify: bool,
    pub poll_interval_secs: u64,
    pub connect_timeout_secs: u64,
    pub op_timeout_secs: u64,
    pub log_level: String,
    /// Console API read token (#360); see the overlay field. A secret.
    pub read_token: Option<String>,
    /// Console API admin token (#360); see the overlay field. A secret.
    pub admin_token: Option<String>,
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        ConsoleConfig {
            http_addr: DEFAULT_HTTP_ADDR.to_owned(),
            seeds: Vec::new(),
            prometheus_url: None,
            node_user: None,
            node_password_file: None,
            node_tls: false,
            node_tls_ca: None,
            node_tls_insecure_skip_verify: false,
            poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            op_timeout_secs: DEFAULT_OP_TIMEOUT_SECS,
            log_level: DEFAULT_LOG_LEVEL.to_owned(),
            read_token: None,
            admin_token: None,
        }
    }
}

impl ConsoleConfig {
    /// Fold the overlays (lowest precedence first) onto the defaults. Infallible:
    /// every layer has already been parsed into typed `Option`s.
    #[must_use]
    pub fn resolve(overlays: &[ConsoleConfigOverlay]) -> Self {
        let mut cfg = ConsoleConfig::default();
        for o in overlays {
            if let Some(v) = &o.http_addr {
                cfg.http_addr.clone_from(v);
            }
            if let Some(v) = &o.seeds {
                cfg.seeds.clone_from(v);
            }
            if let Some(v) = &o.prometheus_url {
                cfg.prometheus_url = Some(v.clone());
            }
            if let Some(v) = &o.node_user {
                cfg.node_user = Some(v.clone());
            }
            if let Some(v) = &o.node_password_file {
                cfg.node_password_file = Some(v.clone());
            }
            if let Some(v) = o.node_tls {
                cfg.node_tls = v;
            }
            if let Some(v) = &o.node_tls_ca {
                cfg.node_tls_ca = Some(v.clone());
            }
            if let Some(v) = o.node_tls_insecure_skip_verify {
                cfg.node_tls_insecure_skip_verify = v;
            }
            if let Some(v) = o.poll_interval_secs {
                cfg.poll_interval_secs = v;
            }
            if let Some(v) = o.connect_timeout_secs {
                cfg.connect_timeout_secs = v;
            }
            if let Some(v) = o.op_timeout_secs {
                cfg.op_timeout_secs = v;
            }
            if let Some(v) = &o.log_level {
                cfg.log_level.clone_from(v);
            }
            if let Some(v) = &o.read_token {
                cfg.read_token = Some(v.clone());
            }
            if let Some(v) = &o.admin_token {
                cfg.admin_token = Some(v.clone());
            }
        }
        cfg
    }

    /// Validate the effective config. Hard errors stop boot; softer concerns are
    /// logged as warnings (so a partially-configured console still boots for
    /// local inspection in PR-1).
    pub fn validate(&self) -> Result<(), ConsoleConfigError> {
        if self.http_addr.trim().is_empty() {
            return Err(ConsoleConfigError::Invalid {
                field: "http_addr",
                reason: "must be a non-empty host:port".to_owned(),
            });
        }
        if !self.http_addr.contains(':') {
            return Err(ConsoleConfigError::Invalid {
                field: "http_addr",
                reason: format!("expected host:port, got '{}'", self.http_addr),
            });
        }
        if self.poll_interval_secs == 0 {
            return Err(ConsoleConfigError::Invalid {
                field: "poll_interval_secs",
                reason: "must be at least 1".to_owned(),
            });
        }
        if self.connect_timeout_secs == 0 {
            return Err(ConsoleConfigError::Invalid {
                field: "connect_timeout_secs",
                reason: "must be at least 1 (a connect must be bounded)".to_owned(),
            });
        }
        if self.op_timeout_secs == 0 {
            return Err(ConsoleConfigError::Invalid {
                field: "op_timeout_secs",
                reason: "must be at least 1 (every node read must be bounded)".to_owned(),
            });
        }
        // TLS must AUTHENTICATE the peer by default: with TLS on and no CA, refuse
        // to boot unless the operator EXPLICITLY opted into accept-any-cert. This
        // closes the silent accept-any path (a MITM could otherwise impersonate a
        // node and capture the AUTH credential).
        if self.node_tls && self.node_tls_ca.is_none() && !self.node_tls_insecure_skip_verify {
            return Err(ConsoleConfigError::Invalid {
                field: "node_tls",
                reason: "node_tls requires node_tls_ca to verify the peer, or set \
                         node_tls_insecure_skip_verify=true to accept any cert (INSECURE, \
                         MITM-exposed)"
                    .to_owned(),
            });
        }
        if self.node_tls && self.node_tls_insecure_skip_verify {
            tracing::warn!(
                "node_tls_insecure_skip_verify is set: node TLS certificates are NOT verified; the \
                 link is encrypted but exposed to an active MITM. Supply node_tls_ca instead."
            );
        }
        if self.node_tls_ca.is_some() && !self.node_tls {
            tracing::warn!("node_tls_ca is set but node_tls is false; the CA bundle is unused");
        }
        if self.node_user.is_some() && self.node_password_file.is_none() {
            tracing::warn!(
                "node_user is set but node_password_file is not; node AUTH will have no password"
            );
        }
        // A token configured as whitespace-only is treated as UNSET by the auth
        // policy (a blank token must not silently authenticate everyone); warn so
        // the operator is not surprised that enforcement did not turn on.
        if self
            .read_token
            .as_ref()
            .is_some_and(|t| t.trim().is_empty())
        {
            tracing::warn!(
                "read_token is set but blank; it is ignored (the console will not enforce on it)"
            );
        }
        if self
            .admin_token
            .as_ref()
            .is_some_and(|t| t.trim().is_empty())
        {
            tracing::warn!(
                "admin_token is set but blank; it is ignored (the console will not enforce on it)"
            );
        }
        Ok(())
    }

    /// A human-readable dump of the effective config for the `config` subcommand.
    /// Shows only references (paths), never a secret value.
    #[must_use]
    pub fn describe(&self) -> String {
        let opt = |o: &Option<String>| o.clone().unwrap_or_else(|| "(none)".to_owned());
        let optp = |o: &Option<PathBuf>| {
            o.as_ref()
                .map_or_else(|| "(none)".to_owned(), |p| p.display().to_string())
        };
        let seeds = if self.seeds.is_empty() {
            "(none)".to_owned()
        } else {
            self.seeds.join(", ")
        };
        // A token is a SECRET: show only whether it is set, never the value. A
        // whitespace-only token is reported as not set (the auth policy ignores it).
        let token_state = |o: &Option<String>| -> &'static str {
            if o.as_ref().is_some_and(|t| !t.trim().is_empty()) {
                "(set)"
            } else {
                "(none)"
            }
        };
        format!(
            "http_addr          = {}\n\
             seeds              = {}\n\
             prometheus_url     = {}\n\
             node_user          = {}\n\
             node_password_file = {}\n\
             node_tls           = {}\n\
             node_tls_ca        = {}\n\
             node_tls_insecure_skip_verify = {}\n\
             poll_interval_secs = {}\n\
             connect_timeout_secs = {}\n\
             op_timeout_secs    = {}\n\
             log_level          = {}\n\
             read_token         = {}\n\
             admin_token        = {}\n",
            self.http_addr,
            seeds,
            opt(&self.prometheus_url),
            opt(&self.node_user),
            optp(&self.node_password_file),
            self.node_tls,
            optp(&self.node_tls_ca),
            self.node_tls_insecure_skip_verify,
            self.poll_interval_secs,
            self.connect_timeout_secs,
            self.op_timeout_secs,
            self.log_level,
            token_state(&self.read_token),
            token_state(&self.admin_token),
        )
    }
}

/// Split a comma-separated seed list, trimming whitespace and dropping empties.
fn parse_seed_list(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Parse a non-negative integer config field, mapping a failure to the typed
/// [`ConsoleConfigError::Invalid`] with the field name.
fn parse_u64(field: &'static str, v: &str) -> Result<u64, ConsoleConfigError> {
    v.trim()
        .parse::<u64>()
        .map_err(|e| ConsoleConfigError::Invalid {
            field,
            reason: format!("not a valid non-negative integer: {e}"),
        })
}

/// Parse a permissive boolean (`true/false/1/0/yes/no/on/off`, case-insensitive).
fn parse_bool(field: &'static str, v: &str) -> Result<bool, ConsoleConfigError> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(ConsoleConfigError::Invalid {
            field,
            reason: format!("not a boolean: '{other}'"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let cfg = ConsoleConfig::default();
        assert_eq!(cfg.http_addr, "127.0.0.1:9180");
        assert!(cfg.seeds.is_empty());
        assert!(!cfg.node_tls);
        assert_eq!(cfg.poll_interval_secs, 10);
        assert_eq!(cfg.connect_timeout_secs, 5);
        assert_eq!(cfg.op_timeout_secs, 5);
        cfg.validate().unwrap();
    }

    #[test]
    fn toml_overlay_parses_and_resolves() {
        let toml = r#"
            http_addr = "127.0.0.1:8080"
            seeds = ["10.0.0.1:6379", "10.0.0.2:6379"]
            prometheus_url = "http://prom:9090"
            node_user = "console_monitor"
            node_password_file = "/run/secrets/console_pw"
            node_tls = true
            node_tls_ca = "/etc/ssl/ca.pem"
            poll_interval_secs = 5
            connect_timeout_secs = 3
            op_timeout_secs = 7
        "#;
        let overlay = ConsoleConfigOverlay::from_toml_str(toml).unwrap();
        let cfg = ConsoleConfig::resolve(&[overlay]);
        assert_eq!(cfg.http_addr, "127.0.0.1:8080");
        assert_eq!(cfg.seeds, vec!["10.0.0.1:6379", "10.0.0.2:6379"]);
        assert_eq!(cfg.prometheus_url.as_deref(), Some("http://prom:9090"));
        assert_eq!(cfg.node_user.as_deref(), Some("console_monitor"));
        assert!(cfg.node_tls);
        assert_eq!(cfg.poll_interval_secs, 5);
        assert_eq!(cfg.connect_timeout_secs, 3);
        assert_eq!(cfg.op_timeout_secs, 7);
        cfg.validate().unwrap();
    }

    #[test]
    fn validate_rejects_zero_timeouts() {
        let cfg = ConsoleConfig {
            connect_timeout_secs: 0,
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConsoleConfigError::Invalid {
                field: "connect_timeout_secs",
                ..
            })
        ));
        let cfg = ConsoleConfig {
            op_timeout_secs: 0,
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConsoleConfigError::Invalid {
                field: "op_timeout_secs",
                ..
            })
        ));
    }

    #[test]
    fn unknown_toml_field_is_rejected() {
        let err = ConsoleConfigOverlay::from_toml_str("bogus = 1").unwrap_err();
        assert!(matches!(err, ConsoleConfigError::Toml(_)));
    }

    #[test]
    fn later_overlay_wins() {
        let lower = ConsoleConfigOverlay {
            http_addr: Some("0.0.0.0:1111".to_owned()),
            ..Default::default()
        };
        let higher = ConsoleConfigOverlay {
            http_addr: Some("0.0.0.0:2222".to_owned()),
            ..Default::default()
        };
        let cfg = ConsoleConfig::resolve(&[lower, higher]);
        assert_eq!(cfg.http_addr, "0.0.0.0:2222");
    }

    #[test]
    fn validate_rejects_bad_http_addr_and_zero_interval() {
        let cfg = ConsoleConfig {
            http_addr: "nope".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConsoleConfigError::Invalid {
                field: "http_addr",
                ..
            })
        ));
        let cfg = ConsoleConfig {
            poll_interval_secs: 0,
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConsoleConfigError::Invalid {
                field: "poll_interval_secs",
                ..
            })
        ));
    }

    #[test]
    fn seed_list_parses_and_trims() {
        assert_eq!(
            parse_seed_list(" a:1 , b:2 ,, c:3 "),
            vec!["a:1", "b:2", "c:3"]
        );
        assert!(parse_seed_list("").is_empty());
    }

    #[test]
    fn bool_parser_is_permissive() {
        for t in ["true", "1", "YES", "On"] {
            assert!(parse_bool("f", t).unwrap());
        }
        for f in ["false", "0", "no", "OFF"] {
            assert!(!parse_bool("f", f).unwrap());
        }
        assert!(parse_bool("f", "maybe").is_err());
    }

    #[test]
    fn describe_hides_no_secret_and_lists_fields() {
        let cfg = ConsoleConfig::default();
        let text = cfg.describe();
        assert!(text.contains("http_addr"));
        assert!(text.contains("seeds              = (none)"));
        assert!(text.contains("node_tls           = false"));
        assert!(text.contains("node_tls_insecure_skip_verify = false"));
    }

    #[test]
    fn validate_rejects_node_tls_without_ca_or_insecure_optout() {
        // TLS on, no CA, no explicit insecure opt-out -> hard error (the silent
        // accept-any path is closed).
        let cfg = ConsoleConfig {
            node_tls: true,
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConsoleConfigError::Invalid {
                field: "node_tls",
                ..
            })
        ));
        // TLS on WITH a CA -> ok (verified link).
        let cfg = ConsoleConfig {
            node_tls: true,
            node_tls_ca: Some(PathBuf::from("/etc/ca.pem")),
            ..Default::default()
        };
        cfg.validate().unwrap();
        // TLS on, no CA, but the EXPLICIT insecure opt-out -> ok (encrypted but
        // unverified; the operator accepted the MITM exposure).
        let cfg = ConsoleConfig {
            node_tls: true,
            node_tls_insecure_skip_verify: true,
            ..Default::default()
        };
        cfg.validate().unwrap();
    }

    #[test]
    fn tokens_resolve_from_toml_and_describe_redacts_the_value() {
        let toml = r#"
            read_token = "super-secret-read"
            admin_token = "super-secret-admin"
        "#;
        let overlay = ConsoleConfigOverlay::from_toml_str(toml).unwrap();
        let cfg = ConsoleConfig::resolve(&[overlay]);
        assert_eq!(cfg.read_token.as_deref(), Some("super-secret-read"));
        assert_eq!(cfg.admin_token.as_deref(), Some("super-secret-admin"));
        // The describe dump shows ONLY whether a token is set, never the value.
        let text = cfg.describe();
        assert!(text.contains("read_token         = (set)"), "{text}");
        assert!(text.contains("admin_token        = (set)"), "{text}");
        assert!(!text.contains("super-secret-read"), "{text}");
        assert!(!text.contains("super-secret-admin"), "{text}");
    }

    #[test]
    fn describe_reports_unset_and_blank_tokens_as_none() {
        let cfg = ConsoleConfig::default();
        assert!(cfg.describe().contains("read_token         = (none)"));
        assert!(cfg.describe().contains("admin_token        = (none)"));
        // A blank token is reported as (none): the auth policy ignores it.
        let cfg = ConsoleConfig {
            read_token: Some("   ".to_owned()),
            ..Default::default()
        };
        assert!(cfg.describe().contains("read_token         = (none)"));
    }

    #[test]
    fn insecure_skip_verify_resolves_from_overlay_and_env_name() {
        let toml = r"
            node_tls = true
            node_tls_insecure_skip_verify = true
        ";
        let overlay = ConsoleConfigOverlay::from_toml_str(toml).unwrap();
        let cfg = ConsoleConfig::resolve(&[overlay]);
        assert!(cfg.node_tls_insecure_skip_verify);
        cfg.validate().unwrap();
    }
}
