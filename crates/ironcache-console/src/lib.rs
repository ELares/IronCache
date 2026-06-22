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
//! SECURITY: the `/api/*` surface exposes node internals (addresses, slowlog argv
//! = key names, client IPs) and is UNAUTHENTICATED today; it relies on the
//! loopback default bind and MUST move behind the auth/RBAC tier (#360) and the
//! VPN-locked exposure (#369) before the console is exposed.
#![forbid(unsafe_code)]

pub mod api;
pub mod assets;
pub mod cli;
pub mod config;
pub mod http;
pub mod info;
pub mod logging;
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
        let state = http::ConsoleHttpState::with_topology(metrics.clone(), topology.clone());
        let listener = tokio::net::TcpListener::bind(&cfg.http_addr)
            .await
            .with_context(|| format!("binding the console HTTP address {}", cfg.http_addr))?;
        // The process is up: liveness flips and never flips back. Readiness is
        // NOT set here: the poll loop flips it on the FIRST successful node poll
        // (#355/#366), so `/readyz` is 503 until the console has real data.
        state.set_live(true);
        log_boot(cfg);

        // Resolve auth/TLS ONCE at startup (read the password file here, not every
        // tick), then spawn the bounded poll loop. The env clock is the snapshot
        // freshness seam (ADR-0003).
        let clock = Arc::new(SystemEnv::new());
        let auth = poll::resolve_auth(cfg).context("reading the node password file")?;
        let tls = poll::resolve_tls(cfg);
        let poller = tokio::spawn(poll::run_poll_loop(
            clock,
            cfg.clone(),
            metrics.clone(),
            state.clone(),
            topology,
            auth,
            tls,
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
    if binds_all_interfaces(&cfg.http_addr) {
        tracing::warn!(
            addr = %cfg.http_addr,
            "console: bound to a wildcard address (all interfaces); keep it behind a \
             VPN-locked load balancer (see #369), not world-reachable"
        );
    }
}

/// Whether `addr` (a `host:port`) binds all interfaces (`0.0.0.0`, `::`, or an
/// empty host). Used to warn at boot since the console is an admin/monitoring
/// plane that should not be world-reachable.
fn binds_all_interfaces(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.is_empty() || host == "0.0.0.0" || host == "::"
}

#[cfg(test)]
mod tests {
    use super::binds_all_interfaces;

    #[test]
    fn wildcard_host_detection() {
        assert!(binds_all_interfaces("0.0.0.0:9180"));
        assert!(binds_all_interfaces("[::]:9180"));
        assert!(binds_all_interfaces(":9180"));
        assert!(!binds_all_interfaces("127.0.0.1:9180"));
        assert!(!binds_all_interfaces("[::1]:9180"));
        assert!(!binds_all_interfaces("10.2.0.5:9180"));
    }
}
