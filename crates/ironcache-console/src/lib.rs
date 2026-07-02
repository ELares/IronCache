// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache Console library half (epic #352, issue #353).
//!
//! The console is a SEPARATE server from the `ironcache` data-plane binary
//! (the InfluxDB-Enterprise model, ADR-deferred): it discovers an IronCache
//! deployment, aggregates a cluster-wide view, and serves a monitoring
//! dashboard, while staying OUT of the client-to-shard data path. This crate
//! holds the wiring (CLI, config, the bounded HTTP responder, self-metrics) so
//! integration tests under `tests/` can drive the real server; `main.rs` is a
//! thin entry point over [`run_cli`].
//!
//! Scope so far: boot plus wire (a standalone HTTP server with `/livez`,
//! `/readyz`, and the console's OWN `/metrics` so the monitor can be monitored,
//! layered config, structured tracing); node acquisition + the single-node
//! topology view (#355, #366); and now the REST API (#358). The `/api/*` surface
//! serves stable JSON over the polled topology: `/api/health`, `/api/cluster`,
//! `/api/nodes`, `/api/nodes/{addr}`, `/api/slowlog`, `/api/clients`,
//! `/api/keyspace`, and a static `/api/openapi.json`. Node acquisition now also
//! fetches `SLOWLOG GET` and `CLIENT LIST` per node (each resilient: a per-section
//! failure or ACL denial records that section's error and yields a degraded
//! snapshot, never failing the whole acquire). The dashboard SPA (#359) hangs off
//! the same responder at `/`, `/app.css`, and `/app.js` (static assets embedded
//! with `include_str!`, served with strict security headers and a CSP that needs
//! no inline script/style). The aggregation-from-Prometheus layer and TLS
//! hardening land in later PRs (#356, #369).
//!
//! # SECURITY POSTURE (#369)
//!
//! The console is a recon-heavy surface. This section is the single statement of
//! what it exposes, who may see it, and the boundaries that keep it contained.
//!
//! ## Credential blast radius
//!
//! The console holds ONE secret: the node password (a least-privilege ACL user),
//! read from `node_password_file` at connect time and never logged (and held in a
//! `zeroize`-backed buffer). It is read-only/observability-scoped, NOT the cache's
//! data-plane credential, so a console compromise does not yield write access to
//! the cache. The Prometheus URL is server-config, not a credential. What the
//! `/api/*` surface DISCLOSES is the real sensitivity: node addresses, the SLOWLOG
//! argv (which contains KEY NAMES), and connected client IPs. Treat the API output,
//! not just the secret, as the asset to protect.
//!
//! ## The three access tiers (RBAC, #360, see [`crate::auth`])
//!
//! Every `/api/*` route maps to a tier; routing FAILS CLOSED (an unknown or
//! trailing-slash route is treated as privileged):
//!
//! * `Open`: aggregate, non-identifying facts (health, cluster up/down counts, the
//!   OpenAPI doc). Public in every posture.
//! * `PrivilegedRead`: anything exposing addresses, key names, or client IPs.
//!   Requires the read (or admin) token when a token is configured; on a
//!   non-loopback bind with no token it is refused (401).
//! * `Admin`: reserved for phase-2 management verbs (none today).
//!
//! ## The SSRF boundary (the history/Prometheus path)
//!
//! The `/api/timeseries` route queries Prometheus. The boundary that stops it from
//! becoming a server-side request forgery pivot is layered:
//!
//! * The Prometheus base URL is SERVER-CONFIG ONLY; the request never supplies it.
//! * The `metric` parameter is ALLOWLISTED to a bare `ironcache_*` name
//!   ([`crate::history::is_allowed_metric`]); the console builds the PromQL itself,
//!   so raw PromQL / label matchers / `&query=` injection never cross the boundary.
//! * The HTTP client does NOT follow redirects: a 3xx is a typed error, never an
//!   auto-connect to a `Location`-chosen host ([`crate::httpclient`]).
//! * The HTTP client BLOCKS link-local / cloud-metadata addresses after resolution
//!   (`169.254.0.0/16` incl. `169.254.169.254`, and `fe80::/10`), screening the
//!   PARSED IP so the decimal/octal/IPv4-mapped forms cannot bypass it.
//!
//! ## Defense-in-depth on every API response
//!
//! Each `/api/*` response carries `X-Content-Type-Options: nosniff` and
//! `Cache-Control: no-store` (the sensitive JSON must not be content-sniffed or
//! cached); the UI assets carry a strict same-origin CSP. See [`crate::http`].
//!
//! ## Exposure requires BOTH a tier and a locked LB
//!
//! Serving this beyond loopback requires the auth tier (above) AND a VPN-locked
//! load balancer. The deployment hardening (the locked LB, #369) and the
//! least-privilege node ACL user (#367) are the INFRA follow-ups that finish the
//! posture; this crate implements the CODE-side controls.
#![forbid(unsafe_code)]

pub mod api;
pub mod assets;
pub mod auth;
pub mod cli;
/// Cluster topology discovery (#354): fetch + parse the engine's structured `/topology` endpoint
/// (#365) into a typed model, coherent in standalone mode.
pub mod cluster;
pub mod config;
pub mod history;
/// The embedded ring-buffer history source (#370): in-memory trend history without an external
/// Prometheus, behind the same [`history::HistorySource`] interface.
pub mod history_embedded;
pub mod http;
pub mod httpclient;
pub mod info;
pub mod logging;
pub mod manage;
pub mod metrics;
pub mod node;
pub mod poll;
pub mod resp;
pub mod snapshot;

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use ironcache_env::SystemEnv;

use crate::config::{ConsoleConfig, ConsoleConfigOverlay};

/// Whether a token is configured (any non-blank read or admin token). Mirrors the
/// auth policy's enforce decision so `log_boot` can describe the posture without
/// holding the resolved policy.
fn has_token(cfg: &ConsoleConfig) -> bool {
    let set = |o: &Option<String>| o.as_ref().is_some_and(|t| !t.trim().is_empty());
    set(&cfg.read_token) || set(&cfg.admin_token)
}

/// The conventional config path checked when `--config` is not given. An absent
/// file is fine (defaults plus env apply), matching the engine's posture.
const DEFAULT_CONFIG_PATH: &str = "/etc/ironcache/console.toml";

/// Parse-free entry point: dispatch an already-parsed [`cli::Cli`]. `main.rs`
/// calls this with `Cli::parse()`; tests call it with a constructed `Cli`.
pub fn run_cli(cli: &cli::Cli) -> anyhow::Result<()> {
    // Resolve config FIRST, then install tracing at the RESOLVED level, so a
    // `log_level` set in the TOML file / `IRONCACHE_CONSOLE_LOG_LEVEL` actually
    // takes effect (clap no longer forces a CLI default that would mask it). A
    // config-resolution error here surfaces via the process exit + stderr before
    // the subscriber exists, which is acceptable.
    let cfg = resolve_config(cli).context("loading console configuration")?;
    logging::install_tracing(&cfg.log_level);
    // Validate AFTER the subscriber is installed so validate's soft warnings are
    // actually logged; a hard error still aborts boot with a clean message.
    cfg.validate().context("validating console configuration")?;
    match cli.command.unwrap_or(cli::Command::Run) {
        cli::Command::Run => serve(&cfg),
        cli::Command::Check => {
            println!("console config ok");
            Ok(())
        }
        cli::Command::Config => {
            print!("{}", cfg.describe());
            Ok(())
        }
    }
}

/// Resolve (but do not validate) the effective config from the layered sources
/// (lowest to highest precedence): defaults -> TOML file ->
/// `IRONCACHE_CONSOLE_*` env -> CLI flags.
fn resolve_config(cli: &cli::Cli) -> anyhow::Result<ConsoleConfig> {
    let file_overlay = if let Some(path) = &cli.config {
        ConsoleConfigOverlay::from_toml_file(path)
            .with_context(|| format!("loading config file {}", path.display()))?
    } else {
        ConsoleConfigOverlay::from_toml_file(Path::new(DEFAULT_CONFIG_PATH))?
    };
    let env_overlay =
        ConsoleConfigOverlay::from_env().context("reading IRONCACHE_CONSOLE_* env vars")?;
    let cli_overlay = ConsoleConfigOverlay {
        http_addr: cli.http_addr.clone(),
        log_level: cli.log_level.clone(),
        ..Default::default()
    };
    Ok(ConsoleConfig::resolve(&[
        file_overlay,
        env_overlay,
        cli_overlay,
    ]))
}

/// Run the console server: bind the HTTP listener, mark live/ready, and serve
/// until a shutdown signal. The console is a network service, so it runs on a
/// multi-thread tokio runtime.
fn serve(cfg: &ConsoleConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;
    rt.block_on(async {
        let metrics = Arc::new(metrics::ConsoleMetrics::new());
        // The shared topology cell: the poll loop writes it, the HTTP surface
        // (the REST API, #358) reads it.
        let topology = poll::new_topology_holder();
        // Resolve the auth/RBAC policy (#360) from the configured tokens and the
        // bind classification: a token => ENFORCE; no token + loopback => dev
        // (serve all); no token + non-loopback => OPEN only.
        let auth_policy = auth::AuthPolicy::resolve(
            cfg.read_token.as_deref(),
            cfg.admin_token.as_deref(),
            binds_loopback(&cfg.http_addr),
        );
        // The history source (#356): a Prometheus adapter when a `prometheus_url`
        // is configured, else `None` (so `/api/timeseries` answers 503). SECURITY:
        // the base URL comes ONLY from server config here, never from a request.
        let (history, embedded_store) = build_history(cfg);
        // Resolve auth/TLS ONCE at startup (read the password file here, not every
        // tick), shared by the poll loop AND the on-demand management connections.
        let node_auth = poll::resolve_auth(cfg).context("reading the node password file")?;
        let node_tls = poll::resolve_tls(cfg);
        // The on-demand node-connection factory (#361): the management endpoints
        // open a short-lived NodeClient to the FIRST seed per request. `None` when
        // no seed is configured, so the management endpoints answer 503.
        let node_access = build_node_access(cfg, node_auth.clone(), node_tls.clone());
        let state = http::ConsoleHttpState::with_topology_and_auth(
            metrics.clone(),
            topology.clone(),
            auth_policy,
        )
        .with_history(history)
        .with_node_access(node_access);
        let listener = tokio::net::TcpListener::bind(&cfg.http_addr)
            .await
            .with_context(|| format!("binding the console HTTP address {}", cfg.http_addr))?;
        // The process is up: liveness flips and never flips back. Readiness is
        // NOT set here: the poll loop flips it on the FIRST successful node poll
        // (#355/#366), so `/readyz` is 503 until the console has real data.
        state.set_live(true);
        log_boot(cfg);

        // Spawn the bounded poll loop with the auth/TLS resolved above (the password
        // file is read once at startup, not every tick). The env clock is the
        // snapshot freshness seam (ADR-0003).
        let clock = Arc::new(SystemEnv::new());
        let poller = tokio::spawn(poll::run_poll_loop(
            clock,
            cfg.clone(),
            metrics.clone(),
            state.clone(),
            topology,
            node_auth,
            node_tls,
            embedded_store,
        ));

        let result = tokio::select! {
            () = http::accept_loop(listener, state.clone()) => {
                // The accept loop only returns on an unrecoverable listener error.
                Ok(())
            }
            r = tokio::signal::ctrl_c() => {
                r.context("waiting for the shutdown signal")?;
                tracing::info!("console: shutdown signal received; exiting");
                Ok(())
            }
        };
        // Stop the poll loop on the way out.
        poller.abort();
        result
    })
}

/// Build the history source (#356/#370) from config, returning `(source, embedded_store)`:
/// - a [`history::PrometheusSource`] when `prometheus_url` is set (Prometheus wins when both are
///   configured); the `embedded_store` is then `None`;
/// - else, when `history_embedded_hours` is set, an embedded ring-buffer source (#370) plus the
///   shared `Arc<EmbeddedHistory>` the POLL LOOP records into (so the query side and the record side
///   share one buffer);
/// - else `(None, None)` (so `/api/timeseries` answers 503).
///
/// SECURITY: the Prometheus base URL is taken ONLY from server config; a request never supplies it
/// (the SSRF boundary). The query timeouts reuse the node connect/op timeout bounds.
fn build_history(
    cfg: &ConsoleConfig,
) -> (
    Option<Arc<dyn history::HistorySource>>,
    Option<Arc<history_embedded::EmbeddedHistory>>,
) {
    if let Some(url) = cfg.prometheus_url.as_ref() {
        let connect_timeout = std::time::Duration::from_secs(cfg.connect_timeout_secs.max(1));
        let read_timeout = std::time::Duration::from_secs(cfg.op_timeout_secs.max(1));
        let source = history::PrometheusSource::new(url, connect_timeout, read_timeout);
        return (Some(Arc::new(source)), None);
    }
    if let Some(hours) = cfg.history_embedded_hours {
        let retention = std::time::Duration::from_secs(hours.max(1).saturating_mul(3600));
        let store = Arc::new(history_embedded::EmbeddedHistory::new(retention));
        let source = history_embedded::EmbeddedSource::new(Arc::clone(&store));
        return (Some(Arc::new(source)), Some(store));
    }
    (None, None)
}

/// Build the on-demand node-connection factory (#361) from config: the FIRST seed
/// plus the resolved auth/tls and the connect/op timeout bounds, wrapped in an
/// `Arc` for cheap sharing into the HTTP state. `None` when no seed is configured,
/// so the management endpoints answer `503` rather than dialing nothing.
///
/// SECURITY: the management connections AUTH with the SAME least-privilege node
/// ACL user the poller uses, so the node ACL bounds every management command
/// (defense in depth). The password is held in the zeroized `NodeAuth` buffer.
fn build_node_access(
    cfg: &ConsoleConfig,
    auth: Option<crate::node::NodeAuth>,
    tls: Option<crate::node::NodeTls>,
) -> Option<Arc<crate::node::NodeAccess>> {
    let addr = cfg.seeds.first()?.clone();
    Some(Arc::new(crate::node::NodeAccess {
        addr,
        tls,
        auth,
        connect_timeout: std::time::Duration::from_secs(cfg.connect_timeout_secs.max(1)),
        op_timeout: std::time::Duration::from_secs(cfg.op_timeout_secs.max(1)),
    }))
}

/// Emit the one-line boot banner (and a warning if the console has no seed nodes
/// configured yet, which PR-2 will make a hard requirement).
fn log_boot(cfg: &ConsoleConfig) {
    tracing::info!(
        version = cli::BUILD_VERSION,
        addr = %cfg.http_addr,
        seeds = cfg.seeds.len(),
        poll_interval_secs = cfg.poll_interval_secs,
        "console: serving /livez, /readyz, /metrics; polling the seed node"
    );
    if cfg.seeds.is_empty() {
        tracing::warn!(
            "console: no seed nodes configured (IRONCACHE_CONSOLE_SEEDS / [seeds]); the poll loop \
             is idle and /readyz stays not-ready until a seed is set"
        );
    }
    if embedded_history_is_ha_inconsistent(cfg) {
        tracing::warn!(
            addr = %cfg.http_addr,
            "console: EMBEDDED history (history_embedded_hours) on a non-loopback bind is PER-REPLICA \
             and not shared; behind a load balancer (the #363 HA topology) each replica shows a \
             different /api/timeseries window and a replica loss drops its window. Point every \
             replica at a shared `prometheus_url` for HA-consistent history."
        );
    }
    if binds_all_interfaces(&cfg.http_addr) {
        tracing::warn!(
            addr = %cfg.http_addr,
            "console: bound to a wildcard address (all interfaces); keep it behind a \
             VPN-locked load balancer (see #369), not world-reachable"
        );
    }
    log_auth_posture(cfg);
}

/// Emit the one-time auth/RBAC posture banner (#360), so the operator sees at
/// boot exactly how the `/api/*` surface is protected. NEVER logs a token value.
///
///   * a token configured           -> ENFORCE (Bearer-token RBAC across tiers).
///   * no token + loopback bind      -> DEV mode: unauthenticated, loopback-trusted
///     (a prominent one-time warning).
///   * no token + non-loopback bind  -> EXPOSED: OPEN tier only; privileged routes
///     return 401 until a token is configured (a prominent boot warning).
fn log_auth_posture(cfg: &ConsoleConfig) {
    if has_token(cfg) {
        tracing::info!(
            "console: API auth ENFORCED (#360); privileged routes require an Authorization: \
             Bearer token. The UI login flow that sends the token from the browser is a \
             follow-up; on the loopback dev default the dashboard works unauthenticated"
        );
    } else if binds_loopback(&cfg.http_addr) {
        tracing::warn!(
            addr = %cfg.http_addr,
            "console: API is UNAUTHENTICATED (#360); no read_token/admin_token configured and the \
             bind is loopback, so all tiers are served (loopback-trusted dev mode). Configure a \
             token before exposing the console"
        );
    } else {
        tracing::warn!(
            addr = %cfg.http_addr,
            "console: API auth is UNCONFIGURED on a NON-loopback bind (#360); only the OPEN tier \
             is served and privileged routes (node addresses, slowlog key names, client IPs) \
             return 401. Configure read_token or admin_token to enable them"
        );
    }
}

/// Whether `addr` (a `host:port`) binds all interfaces (`0.0.0.0`, `::`, or an
/// empty host). Used to warn at boot since the console is an admin/monitoring
/// plane that should not be world-reachable.
fn binds_all_interfaces(addr: &str) -> bool {
    let host = host_of(addr);
    host.is_empty() || host == "0.0.0.0" || host == "::"
}

/// Whether the console runs EMBEDDED (per-replica, in-memory) history on a non-loopback bind, the
/// one config combination that makes horizontal scaling behind a load balancer (#363 HA) surprising:
/// each replica keeps its OWN `/api/timeseries` trend window (fed only by its own poll loop), so
/// behind an LB the window a client sees depends on which replica answered, and a replica loss drops
/// its window. This is NOT a correctness bug (the cache data path is untouched and every replica is
/// otherwise stateless: header-token auth, independently-polled topology, per-replica readiness) but
/// an operator surprise, so `log_boot` warns and recommends a SHARED `prometheus_url` as the
/// HA-consistent history backend. Embedded history is used only when `prometheus_url` is unset
/// (see [`build_history`]), and a loopback (single-host / dev) bind is not an HA topology, so both are
/// excluded here.
fn embedded_history_is_ha_inconsistent(cfg: &ConsoleConfig) -> bool {
    cfg.prometheus_url.is_none()
        && cfg.history_embedded_hours.is_some()
        && !binds_loopback(&cfg.http_addr)
}

/// Whether `addr` (a `host:port`) binds a LOOPBACK address (the IPv4 `127.0.0.0/8`
/// block or the IPv6 `::1`). Used by the auth/RBAC safe-default posture (#360): a
/// no-token loopback bind is dev-trusted (serve all tiers), while a no-token
/// non-loopback bind serves only the OPEN tier. A wildcard or empty host is NOT
/// loopback (it accepts connections from any interface). Conservative: anything it
/// cannot parse as a loopback literal is treated as NON-loopback (fail closed, so
/// the privileged tiers are gated rather than silently exposed).
fn binds_loopback(addr: &str) -> bool {
    let host = host_of(addr);
    if host.is_empty() {
        return false;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        // A hostname (not an IP literal): only the conventional `localhost` is
        // treated as loopback; any other name is NOT (fail closed).
        Err(_) => host.eq_ignore_ascii_case("localhost"),
    }
}

/// The host portion of a `host:port` (stripping `[..]` IPv6 brackets). For a
/// bracketed IPv6 the split is on the LAST colon after the bracket; for a bare
/// host:port it is the last colon.
fn host_of(addr: &str) -> &str {
    let host = if let Some(rest) = addr.strip_prefix('[') {
        // Bracketed IPv6: the host is up to the closing bracket.
        rest.split_once(']').map_or(rest, |(h, _)| h)
    } else {
        addr.rsplit_once(':').map_or(addr, |(h, _)| h)
    };
    host.trim_start_matches('[').trim_end_matches(']')
}

#[cfg(test)]
mod tests {
    use super::{binds_all_interfaces, binds_loopback};

    #[test]
    fn wildcard_host_detection() {
        assert!(binds_all_interfaces("0.0.0.0:9180"));
        assert!(binds_all_interfaces("[::]:9180"));
        assert!(binds_all_interfaces(":9180"));
        assert!(!binds_all_interfaces("127.0.0.1:9180"));
        assert!(!binds_all_interfaces("[::1]:9180"));
        assert!(!binds_all_interfaces("10.2.0.5:9180"));
    }

    #[test]
    fn loopback_host_detection() {
        // IPv4 loopback block (the default bind and the rest of 127.0.0.0/8).
        assert!(binds_loopback("127.0.0.1:9180"));
        assert!(binds_loopback("127.5.6.7:9180"));
        // IPv6 loopback, bracketed.
        assert!(binds_loopback("[::1]:9180"));
        // localhost name.
        assert!(binds_loopback("localhost:9180"));
        assert!(binds_loopback("LOCALHOST:9180"));
        // NON-loopback: wildcard, routable, empty, and unknown hostnames fail
        // closed (treated as non-loopback so privileged tiers are gated).
        assert!(!binds_loopback("0.0.0.0:9180"));
        assert!(!binds_loopback("[::]:9180"));
        assert!(!binds_loopback(":9180"));
        assert!(!binds_loopback("10.2.0.5:9180"));
        assert!(!binds_loopback("console.internal:9180"));
    }

    #[test]
    fn has_token_treats_blank_as_unset() {
        use super::has_token;
        use crate::config::ConsoleConfig;
        assert!(!has_token(&ConsoleConfig::default()));
        assert!(has_token(&ConsoleConfig {
            read_token: Some("t".to_owned()),
            ..Default::default()
        }));
        assert!(!has_token(&ConsoleConfig {
            admin_token: Some("   ".to_owned()),
            ..Default::default()
        }));
    }

    #[test]
    fn embedded_history_ha_inconsistency_detection() {
        use super::embedded_history_is_ha_inconsistent;
        use crate::config::ConsoleConfig;

        // Embedded history + a non-loopback (HA) bind = the one flagged combination.
        assert!(embedded_history_is_ha_inconsistent(&ConsoleConfig {
            http_addr: "0.0.0.0:9180".to_owned(),
            prometheus_url: None,
            history_embedded_hours: Some(24),
            ..Default::default()
        }));
        assert!(embedded_history_is_ha_inconsistent(&ConsoleConfig {
            http_addr: "10.2.0.5:9180".to_owned(),
            prometheus_url: None,
            history_embedded_hours: Some(6),
            ..Default::default()
        }));
        // A SHARED prometheus backend is HA-consistent, embedded or not -> no warning.
        assert!(!embedded_history_is_ha_inconsistent(&ConsoleConfig {
            http_addr: "0.0.0.0:9180".to_owned(),
            prometheus_url: Some("http://prom:9090".to_owned()),
            history_embedded_hours: Some(24),
            ..Default::default()
        }));
        // A loopback (single-host / dev) bind is not an HA topology -> no warning.
        assert!(!embedded_history_is_ha_inconsistent(&ConsoleConfig {
            http_addr: "127.0.0.1:9180".to_owned(),
            prometheus_url: None,
            history_embedded_hours: Some(24),
            ..Default::default()
        }));
        // No history at all -> nothing to warn about.
        assert!(!embedded_history_is_ha_inconsistent(&ConsoleConfig {
            http_addr: "0.0.0.0:9180".to_owned(),
            prometheus_url: None,
            history_embedded_hours: None,
            ..Default::default()
        }));
    }
}
