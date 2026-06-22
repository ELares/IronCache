// SPDX-License-Identifier: MIT OR Apache-2.0
//! The node poll loop (issue #355): every `poll_interval_secs`, acquire the seed
//! node into a [`Topology`], publish it to a shared holder, record the
//! success/failure self-metric, and flip readiness on the FIRST success.
//!
//! The MVP polls ONE node (the first configured seed); cluster discovery across
//! all seeds lands later. A poll is a SUCCESS when the assembled topology has at
//! least one reachable node; an all-unreachable topology is a FAILURE (the
//! failure counter advances and `/readyz` stays not-ready) yet is still published
//! so the REST/UI layers can show the degraded view with its error strings.
//!
//! Readiness is owned HERE: `lib.rs` no longer flips ready at boot, so `/readyz`
//! returns 503 until the console has real data (the first successful poll). The
//! every operation against a node is bounded (connect + per-op timeouts), so a
//! down / hung node can never wedge the loop.
//!
//! ## Determinism (ADR-0003)
//!
//! Time comes from the env [`Clock`] seam (snapshot stamps) and the sleep between
//! polls uses the runtime timer (`tokio::time`), the sanctioned interval seam; no
//! `SystemTime::now` / `Instant::now` / RNG here.

use std::sync::Arc;
use std::time::Duration;

use ironcache_env::Clock;
use tokio::sync::RwLock;

use crate::config::ConsoleConfig;
use crate::http::ConsoleHttpState;
use crate::metrics::ConsoleMetrics;
use crate::node::{self, NodeAuth, NodeTls};
use crate::snapshot::{Topology, acquire_node, single_node_topology};

/// The shared, swappable latest-topology holder. The poll loop writes it; the
/// REST/UI layers (and `ConsoleHttpState`) read it. `None` until the first poll
/// publishes a topology (reachable or degraded).
pub type TopologyHolder = Arc<RwLock<Option<Topology>>>;

/// Construct an empty topology holder (no poll has run yet).
#[must_use]
pub fn new_topology_holder() -> TopologyHolder {
    Arc::new(RwLock::new(None))
}

/// Resolve the node auth from config: read the password file (if any) and pair it
/// with the configured user. Returns `Ok(None)` when no auth is configured.
///
/// Handles the awkward case gracefully: a `node_user` WITHOUT a password file
/// cannot AUTH (`AUTH <user>` with no password is invalid), so we log a warning
/// and proceed WITHOUT auth rather than sending a broken command. A password file
/// without a user AUTHs the default user (`AUTH <pass>`).
///
/// # Errors
///
/// Returns the I/O error if the configured password file cannot be read.
pub fn resolve_auth(cfg: &ConsoleConfig) -> std::io::Result<Option<NodeAuth>> {
    let Some(path) = &cfg.node_password_file else {
        if cfg.node_user.is_some() {
            tracing::warn!(
                "node_user is set but node_password_file is not; connecting WITHOUT AUTH (a \
                 password is required to authenticate)"
            );
        }
        return Ok(None);
    };
    let password = node::read_password_file(path)?;
    Ok(Some(NodeAuth {
        user: cfg.node_user.clone(),
        password,
    }))
}

/// Resolve the node TLS settings from config: `None` when `node_tls` is off.
/// Carries the EXPLICIT `node_tls_insecure_skip_verify` flag (never inferred from
/// the CA being absent), so verification is required by default.
#[must_use]
pub fn resolve_tls(cfg: &ConsoleConfig) -> Option<NodeTls> {
    if !cfg.node_tls {
        return None;
    }
    Some(NodeTls {
        ca_path: cfg.node_tls_ca.as_ref().map(|p| p.display().to_string()),
        insecure_skip_verify: cfg.node_tls_insecure_skip_verify,
    })
}

/// Run the poll loop until cancelled (the task is aborted at shutdown). Each tick:
/// acquire the first seed, publish the topology, update metrics, and (on the first
/// success) flip readiness. With no seeds configured, it logs once and idles
/// (sleeping the interval) so the console still serves `/livez` and `/metrics`.
///
/// `clock` is the env clock seam (snapshot stamps). `cfg` carries the seeds,
/// interval, and timeouts; `auth`/`tls` are pre-resolved (so the password file is
/// read once at startup, not every tick).
pub async fn run_poll_loop<C: Clock>(
    clock: Arc<C>,
    cfg: ConsoleConfig,
    metrics: Arc<ConsoleMetrics>,
    http_state: ConsoleHttpState,
    holder: TopologyHolder,
    auth: Option<NodeAuth>,
    tls: Option<NodeTls>,
) {
    let interval = Duration::from_secs(cfg.poll_interval_secs.max(1));
    let connect_timeout = Duration::from_secs(cfg.connect_timeout_secs.max(1));
    let op_timeout = Duration::from_secs(cfg.op_timeout_secs.max(1));

    if cfg.seeds.is_empty() {
        tracing::warn!(
            "console poll: no seed nodes configured; the poll loop is idle and /readyz stays \
             not-ready until a seed is set"
        );
    }

    loop {
        if let Some(seed) = cfg.seeds.first() {
            let snapshot = acquire_node(
                clock.as_ref(),
                seed,
                tls.as_ref(),
                auth.as_ref(),
                connect_timeout,
                op_timeout,
            )
            .await;
            let topology = single_node_topology(clock.as_ref(), snapshot);
            let reachable = topology.any_reachable();
            // Publish the topology (even a degraded one) so readers see the
            // latest view + its error strings.
            *holder.write().await = Some(topology);
            if reachable {
                metrics.record_poll_success();
                // Readiness flips on the FIRST successful poll and never flips back.
                http_state.set_ready(true);
                tracing::debug!(seed = %seed, "console poll: node reachable; topology refreshed");
            } else {
                metrics.record_poll_failure();
                tracing::warn!(seed = %seed, "console poll: seed node unreachable");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::SystemEnv;
    use std::path::PathBuf;
    use tokio::io::AsyncWriteExt as _;

    fn base_cfg() -> ConsoleConfig {
        ConsoleConfig::default()
    }

    #[test]
    fn resolve_tls_off_by_default() {
        assert!(resolve_tls(&base_cfg()).is_none());
    }

    #[test]
    fn resolve_tls_on_carries_ca() {
        let cfg = ConsoleConfig {
            node_tls: true,
            node_tls_ca: Some(PathBuf::from("/etc/ca.pem")),
            ..Default::default()
        };
        let tls = resolve_tls(&cfg).unwrap();
        assert_eq!(tls.ca_path.as_deref(), Some("/etc/ca.pem"));
        // Verification on by default: the insecure flag is NOT inferred.
        assert!(!tls.insecure_skip_verify);
    }

    #[test]
    fn resolve_tls_carries_explicit_insecure_flag() {
        let cfg = ConsoleConfig {
            node_tls: true,
            node_tls_insecure_skip_verify: true,
            ..Default::default()
        };
        let tls = resolve_tls(&cfg).unwrap();
        assert!(tls.ca_path.is_none());
        assert!(tls.insecure_skip_verify);
    }

    #[test]
    fn resolve_auth_none_without_password_file() {
        // No password file and no user -> no auth, no warning path taken.
        assert!(resolve_auth(&base_cfg()).unwrap().is_none());
        // user set but no password file -> still no auth (graceful), not an error.
        let cfg = ConsoleConfig {
            node_user: Some("monitor".to_owned()),
            ..Default::default()
        };
        assert!(resolve_auth(&cfg).unwrap().is_none());
    }

    #[test]
    fn resolve_auth_reads_password_file() {
        let path = std::env::temp_dir().join(format!(
            "ironcache-console-poll-pw-{}.txt",
            std::process::id()
        ));
        std::fs::write(&path, b"topsecret\n").unwrap();
        let cfg = ConsoleConfig {
            node_user: Some("monitor".to_owned()),
            node_password_file: Some(path.clone()),
            ..Default::default()
        };
        let auth = resolve_auth(&cfg).unwrap().unwrap();
        assert_eq!(auth.user.as_deref(), Some("monitor"));
        assert_eq!(auth.password.as_slice(), b"topsecret");
        let _ = std::fs::remove_file(&path);
    }

    /// With no seeds, the loop publishes nothing and never flips readiness; it just
    /// idles. We run one short tick by aborting after a brief wait.
    #[tokio::test]
    async fn idle_with_no_seeds_never_ready() {
        let clock = Arc::new(SystemEnv::new());
        let cfg = ConsoleConfig {
            poll_interval_secs: 1,
            ..Default::default()
        };
        let metrics = Arc::new(ConsoleMetrics::new());
        let state = ConsoleHttpState::new(metrics.clone());
        let holder = new_topology_holder();
        let task = tokio::spawn(run_poll_loop(
            clock,
            cfg,
            metrics,
            state.clone(),
            holder.clone(),
            None,
            None,
        ));
        // Give the loop a moment to take its first (no-op) tick.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            holder.read().await.is_none(),
            "no seed -> nothing published"
        );
        let readyz = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(readyz.starts_with("HTTP/1.1 503"), "{readyz}");
        task.abort();
    }

    /// A healthy seed: the first poll publishes a reachable topology, records a
    /// success, and flips readiness to 200.
    #[tokio::test]
    async fn healthy_seed_flips_ready_after_first_poll() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // A stub that answers a SINGLE poll (PING + INFO) then idles.
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let body = "redis_version:7.2.0\r\ncluster_enabled:0\r\n";
            let bulk = format!("${}\r\n{body}\r\n", body.len());
            sock.write_all(bulk.as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let clock = Arc::new(SystemEnv::new());
        let cfg = ConsoleConfig {
            seeds: vec![addr],
            poll_interval_secs: 60, // long: we only need the first tick
            connect_timeout_secs: 2,
            op_timeout_secs: 2,
            ..Default::default()
        };
        let metrics = Arc::new(ConsoleMetrics::new());
        let state = ConsoleHttpState::new(metrics.clone());
        let holder = new_topology_holder();
        let task = tokio::spawn(run_poll_loop(
            clock,
            cfg,
            metrics.clone(),
            state.clone(),
            holder.clone(),
            None,
            None,
        ));

        // Poll for readiness to flip (the first tick completes quickly).
        let mut ready = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if state.respond("GET", "/readyz").starts_with(b"HTTP/1.1 200") {
                ready = true;
                break;
            }
        }
        assert!(ready, "readiness must flip after the first successful poll");
        let topo = holder
            .read()
            .await
            .clone()
            .expect("a topology was published");
        assert!(topo.any_reachable());
        assert!(
            metrics
                .render()
                .contains("ironcache_console_poll_success_total 1")
        );
        task.abort();
        server.abort();
    }

    /// A down seed: the poll publishes a degraded (unreachable) topology, records a
    /// failure, and readiness stays 503.
    #[tokio::test]
    async fn down_seed_stays_not_ready_and_records_failure() {
        // A bound-then-dropped port (refused).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let clock = Arc::new(SystemEnv::new());
        let cfg = ConsoleConfig {
            seeds: vec![addr],
            poll_interval_secs: 60,
            connect_timeout_secs: 1,
            op_timeout_secs: 1,
            ..Default::default()
        };
        let metrics = Arc::new(ConsoleMetrics::new());
        let state = ConsoleHttpState::new(metrics.clone());
        let holder = new_topology_holder();
        let task = tokio::spawn(run_poll_loop(
            clock,
            cfg,
            metrics.clone(),
            state.clone(),
            holder.clone(),
            None,
            None,
        ));

        // Wait for the first tick to publish the degraded topology.
        let mut published = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if holder.read().await.is_some() {
                published = true;
                break;
            }
        }
        assert!(published, "a degraded topology must still be published");
        let topo = holder.read().await.clone().unwrap();
        assert!(!topo.any_reachable());
        // Readiness must NOT have flipped.
        assert!(state.respond("GET", "/readyz").starts_with(b"HTTP/1.1 503"));
        assert!(
            metrics
                .render()
                .contains("ironcache_console_poll_failure_total 1")
        );
        task.abort();
    }
}
