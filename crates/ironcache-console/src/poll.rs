// SPDX-License-Identifier: MIT OR Apache-2.0
//! The node poll loop (issue #355): every `poll_interval_secs`, acquire the seed
//! node into a [`Topology`], publish it to a shared holder, record the
//! success/failure self-metric, and flip readiness on the FIRST success.
//!
//! Multi-seed failover (#354): each tick tries the configured seeds IN ORDER and
//! publishes the FIRST one that yields a reachable node (short-circuiting, so a
//! healthy first seed costs one acquire); if every seed is down, the LAST attempt's
//! degraded view is kept. A poll is a SUCCESS when the assembled topology has at
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

/// The floor on the refresh cadence used while a slot migration is in flight (#354): the console
/// never polls FASTER than this even mid-resharding, so it cannot hammer the engine.
const MIGRATION_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The delay before the next poll given the steady `interval` and whether a slot migration is in
/// flight (#354). A migration is the one window where the slot map's ownership is moving, so a
/// console on a slow steady cadence would show a stale owner until the next tick; during one, refresh
/// at `min(interval, MIGRATION_POLL_INTERVAL)` so the new owner is adopted promptly. A steady cadence
/// already faster than the floor is NEVER slowed (the `min`), and the floor stops a fast resharding
/// from turning into an engine-hammering busy-poll. Pure, so the cadence policy is unit-tested.
#[must_use]
fn migration_aware_delay(interval: Duration, migrating: bool) -> Duration {
    if migrating {
        interval.min(MIGRATION_POLL_INTERVAL)
    } else {
        interval
    }
}

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
#[allow(clippy::too_many_arguments)] // a boot-wiring spawn that threads the resolved poll inputs.
pub async fn run_poll_loop<C: Clock>(
    clock: Arc<C>,
    cfg: ConsoleConfig,
    metrics: Arc<ConsoleMetrics>,
    http_state: ConsoleHttpState,
    holder: TopologyHolder,
    auth: Option<NodeAuth>,
    tls: Option<NodeTls>,
    embedded_history: Option<Arc<crate::history_embedded::EmbeddedHistory>>,
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
        // Default to the steady cadence; an in-flight migration shortens it for the next tick.
        let mut next_sleep = interval;
        // Multi-seed failover (#354): one acquisition pass over the configured seeds,
        // returning the topology to publish (the first reachable, or the last attempt if
        // every seed is down). Extracted into [`acquire_failover_topology`] so the
        // failover is integration-tested against fake nodes (#368) without this loop.
        let chosen = acquire_failover_topology(
            clock.as_ref(),
            &cfg.seeds,
            tls.as_ref(),
            auth.as_ref(),
            connect_timeout,
            op_timeout,
        )
        .await;
        if let Some((mut topology, seed)) = chosen {
            // Cluster topology discovery (#354): when the seed's HTTP admin URL is configured, fetch
            // the structured `/topology` (#365) and fold the membership/slots/epoch/raft view in.
            // BEST-EFFORT: a fetch/parse miss leaves `cluster: None` and never affects node
            // reachability (the RESP view stands on its own). `/topology` is coherent even in
            // standalone mode, so this also enriches the single-node deployment.
            if let Some(http_url) = &cfg.node_http_url {
                match crate::cluster::fetch_cluster_topology(http_url, connect_timeout, op_timeout)
                    .await
                {
                    Ok(ct) => topology.cluster = Some(ct),
                    Err(e) => tracing::debug!(
                        url = %http_url,
                        error = %e,
                        "console poll: /topology discovery failed (best-effort; RESP view kept)"
                    ),
                }
            }
            // Embedded history (#370): record each reachable node's headline INFO figures into the
            // ring buffer this tick, so the trend panels have samples without an external Prometheus.
            if let Some(eh) = &embedded_history {
                for node in &topology.nodes {
                    if let Some(info) = &node.info {
                        crate::history_embedded::record_node_samples(
                            eh,
                            &node.addr,
                            info,
                            node.fetched_unixtime,
                        );
                    }
                }
            }
            let reachable = topology.any_reachable();
            // (#354) Is a slot migration in flight in the view we are about to publish? Capture it
            // BEFORE the move so we can shorten the next refresh while ownership is moving.
            let migrating = topology
                .cluster
                .as_ref()
                .is_some_and(crate::cluster::ClusterTopology::migration_in_progress);
            // Publish the topology (even a degraded one) so readers see the
            // latest view + its error strings.
            *holder.write().await = Some(topology);
            next_sleep = migration_aware_delay(interval, migrating);
            if reachable {
                metrics.record_poll_success();
                // Readiness flips on the FIRST successful poll and never flips back.
                http_state.set_ready(true);
                tracing::debug!(
                    seed = %seed,
                    migrating,
                    next_refresh_secs = next_sleep.as_secs(),
                    "console poll: node reachable; topology refreshed"
                );
            } else {
                metrics.record_poll_failure();
                tracing::warn!(
                    seeds = cfg.seeds.len(),
                    "console poll: ALL seed nodes unreachable"
                );
            }
        }
        tokio::time::sleep(next_sleep).await;
    }
}

/// One failover acquisition pass (#354/#447): try `seeds` IN ORDER, acquiring each into
/// a single-node [`Topology`] and stopping at the first reachable one (short-circuit, so
/// a healthy first seed costs exactly one acquire). Returns the topology to PUBLISH per
/// [`pick_published_seed`] (the first reachable, or the last attempt if every seed is
/// down so a degraded view still shows) and the seed it came from; `None` only when there
/// are no seeds. This is the testable core of the poll loop: it is driven against fake
/// nodes (#368) over real sockets without running the infinite [`run_poll_loop`].
async fn acquire_failover_topology<C: Clock>(
    clock: &C,
    seeds: &[String],
    tls: Option<&NodeTls>,
    auth: Option<&NodeAuth>,
    connect_timeout: Duration,
    op_timeout: Duration,
) -> Option<(Topology, String)> {
    let mut attempts: Vec<(Topology, &str)> = Vec::new();
    for seed in seeds {
        let snapshot = acquire_node(clock, seed, tls, auth, connect_timeout, op_timeout).await;
        let topology = single_node_topology(clock, snapshot);
        let reachable = topology.any_reachable();
        attempts.push((topology, seed.as_str()));
        if reachable {
            break;
        }
        tracing::warn!(
            seed = %seed,
            "console poll: seed node unreachable; trying the next seed"
        );
    }
    let flags: Vec<bool> = attempts.iter().map(|(t, _)| t.any_reachable()).collect();
    pick_published_seed(&flags)
        .and_then(|idx| attempts.into_iter().nth(idx))
        .map(|(t, s)| (t, s.to_owned()))
}

/// The seed-failover publish policy (#354): given each configured seed's reachability
/// in seed order, which one's topology do we publish? The FIRST reachable seed, or (if
/// none is reachable) the LAST seed, so a degraded-but-informative view is still shown.
/// `None` only when there are no seeds. Pure, so the run-loop's short-circuit failover
/// is checked against this spec without spinning up nodes.
#[must_use]
fn pick_published_seed(reachable: &[bool]) -> Option<usize> {
    if reachable.is_empty() {
        return None;
    }
    reachable
        .iter()
        .position(|&r| r)
        .or(Some(reachable.len() - 1))
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
    fn migration_aware_delay_speeds_up_only_during_a_migration() {
        let steady = Duration::from_secs(15);
        // No migration: the steady cadence is used unchanged.
        assert_eq!(migration_aware_delay(steady, false), steady);
        // Mid-migration: drop to the 1s floor so the new owner is adopted promptly.
        assert_eq!(migration_aware_delay(steady, true), MIGRATION_POLL_INTERVAL);
        // A steady cadence already at/under the floor is NEVER slowed by the migration path.
        let fast = Duration::from_secs(1);
        assert_eq!(migration_aware_delay(fast, true), fast);
        assert_eq!(migration_aware_delay(fast, false), fast);
    }

    #[test]
    fn pick_published_seed_prefers_the_first_reachable() {
        // No seeds -> nothing to publish.
        assert_eq!(pick_published_seed(&[]), None);
        // A single reachable / single degraded seed -> that one (index 0).
        assert_eq!(pick_published_seed(&[true]), Some(0));
        assert_eq!(pick_published_seed(&[false]), Some(0));
        // The FIRST reachable wins even when a later seed is also up (short-circuit).
        assert_eq!(pick_published_seed(&[true, true]), Some(0));
        assert_eq!(pick_published_seed(&[false, true]), Some(1));
        assert_eq!(pick_published_seed(&[false, false, true]), Some(2));
        assert_eq!(pick_published_seed(&[false, true, true]), Some(1));
    }

    #[test]
    fn pick_published_seed_falls_back_to_the_last_when_all_down() {
        // All seeds unreachable -> keep the LAST attempt's degraded view.
        assert_eq!(pick_published_seed(&[false, false]), Some(1));
        assert_eq!(pick_published_seed(&[false, false, false]), Some(2));
    }

    // ===================== #368: multi-seed failover over real sockets =====================
    // A controllable fake RESP node drives the REAL acquire path (acquire_node ->
    // single_node_topology -> the failover selection) end to end, so the resilience #447
    // added is integration-tested, not only its pure policy fn.

    /// A single-connection fake RESP node: it answers the EXACT acquire_node sequence
    /// (PING -> `+PONG`, INFO -> the bulk body, SLOWLOG GET -> empty array, CLIENT LIST ->
    /// empty bulk) on ONE connection, then closes. The client awaits each reply before the
    /// next command, so a fixed-order reply sequence matches the sequence. Returns its
    /// loopback address + the server task (abort it at the end of the test).
    async fn spawn_up_node(info_body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let Ok((mut sock, _peer)) = listener.accept().await else {
                return;
            };
            let info = format!("${}\r\n{info_body}\r\n", info_body.len());
            let replies: [&[u8]; 4] = [b"+PONG\r\n", info.as_bytes(), b"*0\r\n", b"$0\r\n\r\n"];
            let mut buf = [0u8; 2048];
            for reply in replies {
                if sock.read(&mut buf).await.unwrap_or(0) == 0 {
                    return;
                }
                if sock.write_all(reply).await.is_err() {
                    return;
                }
            }
            // Brief grace so the client reads the last reply cleanly before the close.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        (addr, server)
    }

    /// A loopback address with NO acceptor (bind then DROP the listener), so a connect is
    /// refused: a "down" seed.
    async fn closed_addr() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().to_string()
        // listener dropped here -> the port has no acceptor.
    }

    #[tokio::test]
    async fn failover_skips_a_down_first_seed_and_publishes_the_reachable_one() {
        let env = SystemEnv::new();
        let down = closed_addr().await;
        let (up, server) = spawn_up_node("redis_version:9.9.9\r\nconnected_clients:2\r\n").await;
        let seeds = vec![down, up.clone()];
        let (topo, seed) = acquire_failover_topology(
            &env,
            &seeds,
            None,
            None,
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await
        .expect("a non-empty seed list yields a topology");
        assert!(
            topo.any_reachable(),
            "failed over to the reachable second seed"
        );
        assert_eq!(seed, up, "published the up seed, not the down first one");
        server.abort();
    }

    #[tokio::test]
    async fn failover_with_all_seeds_down_publishes_a_degraded_view_not_nothing() {
        let env = SystemEnv::new();
        let seeds = vec![closed_addr().await, closed_addr().await];
        let (topo, _seed) = acquire_failover_topology(
            &env,
            &seeds,
            None,
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .expect("a degraded view is still published when every seed is down");
        assert!(
            !topo.any_reachable(),
            "all seeds down -> an unreachable (degraded) view, not a fabricated reachable one"
        );
        assert!(
            !topo.nodes.is_empty(),
            "the degraded view still lists the node + its error for the UI"
        );
    }

    #[tokio::test]
    async fn failover_with_no_seeds_is_none() {
        let env = SystemEnv::new();
        let none = acquire_failover_topology(
            &env,
            &[],
            None,
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await;
        assert!(none.is_none(), "no seeds -> nothing to publish");
    }

    #[tokio::test]
    async fn a_single_healthy_seed_is_reachable_with_parsed_info() {
        let env = SystemEnv::new();
        let (up, server) = spawn_up_node("redis_version:7.7.7\r\nconnected_clients:5\r\n").await;
        let (topo, seed) = acquire_failover_topology(
            &env,
            std::slice::from_ref(&up),
            None,
            None,
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await
        .expect("one healthy seed yields a topology");
        assert!(topo.any_reachable());
        assert_eq!(seed, up);
        // The INFO the fake served was parsed into the node view.
        let node = topo.nodes.first().expect("the reachable node is listed");
        assert!(node.reachable);
        server.abort();
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
        // A stub that answers a SINGLE poll (PING + INFO + SLOWLOG + CLIENT LIST)
        // then idles. The acquire now fetches the rich sections too, so the stub
        // must reply to all four or the poll's first tick would block on them.
        let server = tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            // PING
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            // INFO
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            let body = "redis_version:7.2.0\r\ncluster_enabled:0\r\n";
            let bulk = format!("${}\r\n{body}\r\n", body.len());
            sock.write_all(bulk.as_bytes()).await.unwrap();
            // SLOWLOG GET -> empty array.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"*0\r\n").await.unwrap();
            // CLIENT LIST -> empty bulk.
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                .await
                .unwrap();
            sock.write_all(b"$0\r\n\r\n").await.unwrap();
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
