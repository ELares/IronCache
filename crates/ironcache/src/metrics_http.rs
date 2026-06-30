// SPDX-License-Identifier: MIT OR Apache-2.0
//! The out-of-band operations HTTP endpoint (OBSERVABILITY.md, #152): a bounded, hand-rolled
//! tokio HTTP/1.1 responder bound on `--metrics-addr` that serves
//!
//!   * `GET /metrics` -> the Prometheus text exposition of the cross-shard counter rollup plus
//!     the process gauges (uptime, jemalloc memory, keyspace, maxmemory, persistence, and in
//!     raft-mode the control-plane role/term/commit/voters),
//!   * `GET /livez`   -> `200 OK` once the process is up (a Kubernetes liveness probe), and
//!   * `GET /readyz`  -> `200 OK` when load-on-boot has finished AND (in raft-mode) a leader is
//!     recognized, else `503` with a short reason (a readiness probe).
//!
//! ## Why hand-rolled instead of a web framework
//!
//! The surface is three fixed routes with no body parsing, no routing tree, and no middleware,
//! so a full HTTP stack (hyper/axum) would be dead weight on the static musl/aarch64 build. A
//! minimal tokio reader that bounds the request (a whole-request deadline + a small header cap +
//! a connection-concurrency cap) and matches the request line is enough, adds NO new third-party
//! crate, and keeps the dependency tree pure-Rust (ADR-0017). It is NOT a general HTTP server:
//! anything malformed/oversized is answered with a fixed `400`/`413` and the connection is closed.
//!
//! ## Default-off and the hot path
//!
//! This listener is spawned ONLY when `--metrics-addr` is set (see [`spawn_metrics_server`]). On
//! the DEFAULT path it never runs, no socket is bound, and the server boot is byte-identical.
//! The `/metrics` handler READS existing atomics (the per-shard [`MetricsRegistry`] cells, the
//! jemalloc mallctl, the persistence atomics, the raft status watch); it never touches the
//! command hot path, takes no shard lock, and adds no per-command cost.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ironcache_env::{Clock, Monotonic, SystemEnv};
use ironcache_observe::{MetricsGauges, MetricsRegistry, RaftGauges};
use ironcache_store::process_memory;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// The maximum request bytes the responder will read before rejecting with `413` (a tiny cap:
/// the probes send only a request line + a few headers, never a body). Bounds the per-connection
/// buffer so the endpoint cannot be driven to allocate on a hostile client.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// The WHOLE-REQUEST deadline: the ENTIRE request-read phase (every `read` plus the parse) must
/// complete within this window, else the connection is dropped. Unlike a per-`read` timeout (which a
/// slow-drip client resets on every byte, holding the socket for hours up to the size cap), this is
/// a single deadline over the whole read loop, so a slowloris dribble cannot extend the hold.
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// The maximum number of metrics connections served concurrently. The accept loop holds a
/// [`tokio::sync::Semaphore`] of this many permits and acquires one (non-blocking) before spawning a
/// `serve_conn` task, so a flood of connections cannot accumulate unbounded parked tasks (a task
/// DoS). Small: the ops port serves scrapes + probes, never a high fan-in. At capacity the EXCESS
/// connection is dropped/closed IMMEDIATELY (see [`accept_loop`]) rather than queued without bound or
/// blocking the accept loop.
const MAX_CONCURRENT_CONNS: usize = 128;

/// The shared state the metrics HTTP handler reads at scrape time. Cloned (`Arc` inside) into the
/// accept loop and each connection task. Every field is a cheap, lock-free read; the heavy
/// `RaftHandle`/`PersistState` are the same handles the serve layer already shares by `Arc`.
#[derive(Clone)]
pub struct MetricsState {
    /// The per-shard counter registry; `aggregate()` sums every shard's cell across threads.
    registry: MetricsRegistry,
    /// Liveness: set `true` at the END of boot (the process is serving). `/livez` returns 200
    /// once this is set; it never flips back (a liveness probe answers "is the process up").
    live: Arc<AtomicBool>,
    /// Readiness: set `true` when load-on-boot has finished AND (raft-mode) a leader is known.
    /// `/readyz` returns 200 when set, 503 otherwise. Distinct from `live` so a booted-but-not
    /// -ready node (still loading a snapshot, or a raft node with no leader yet) is correctly
    /// kept out of a load-balancer rotation.
    ready: Arc<ReadyState>,
    /// The metrics task's OWN env (the determinism seam, ADR-0003): its monotonic origin anchors
    /// `uptime` and its clock answers each scrape. `SystemEnv::now()` takes `&self`, so a shared
    /// `Arc<ClockState>` needs NO lock; it is read only on the metrics path, never the command
    /// hot path.
    clock: Arc<ClockState>,
    /// The bound TCP (RESP) port and the shard count, for the `ironcache_shards` gauge and the
    /// boot facts.
    shards: u64,
    /// The current effective `maxmemory` ceiling source (a cheap atomic load each scrape).
    maxmemory: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// The raft control-plane handle, `Some` only in raft-governance mode; read for the
    /// `ironcache_raft_*` gauges and the `/readyz` leader gate.
    raft: Option<ironcache_server::RaftHandle>,
    /// The persistence state, `Some` only when a `data_dir` is configured; read for the
    /// last-save + dirty gauges.
    persist: Option<Arc<crate::persist::PersistState>>,
    /// The structured-topology read state (#365): membership/slots/epoch + node identity, served as
    /// JSON at `/topology` (coherent single-node answer in standalone mode). Read-only.
    topology: crate::topology::TopologyHandle,
}

/// The boot-anchored clock for uptime. One [`SystemEnv`] whose monotonic `origin` is captured at
/// metrics-server start; `uptime_secs` is `now - boot` through that same env (the origins must
/// match, so the boot instant and every later read come from THIS env).
struct ClockState {
    env: SystemEnv,
    boot: Monotonic,
}

impl ClockState {
    fn new() -> Self {
        let env = SystemEnv::new();
        let boot = env.now();
        ClockState { env, boot }
    }

    fn uptime_secs(&self) -> u64 {
        self.env
            .now()
            .saturating_duration_since(self.boot)
            .as_secs()
    }
}

/// Readiness flags, gated separately so `/readyz` can report WHY it is not ready. Load-on-boot is
/// done once EVERY shard's load-on-boot (`coordinator::load_shard_on_boot`) has RETURNED -- whether
/// it restored a snapshot or was a no-op because persistence is off; the raft leader gate is
/// evaluated live from the `RaftHandle` at scrape time (a node can lose its leader after boot), so
/// it is not a stored flag.
///
/// ## Why a per-shard countdown rather than a single flag
///
/// `coordinator::run_drain_loop` only SPAWNS the shard threads and returns; each shard's snapshot
/// restore runs ASYNC on its own executor AFTER the boot wiring returns. A single boot-time flag
/// flipped right after `run_server_observed` returns would therefore report READY while shards are
/// still loading -- on a persistence node with a sizeable snapshot, `/readyz` would answer 200 over
/// an EMPTY/PARTIAL keyspace and k8s would route traffic to it. So readiness AND-reduces a per-shard
/// signal: `load_pending` starts at the shard count, each shard decrements it AFTER its
/// `load_shard_on_boot` completes, and load-on-boot is "done" only when it reaches 0. With
/// persistence OFF every shard's load is an immediate no-op, so the counter drains to 0 promptly and
/// readiness flips fast (no behavior change vs the no-persistence case).
#[derive(Debug)]
pub struct ReadyState {
    /// The number of shards that have NOT YET finished load-on-boot. Initialized to the shard count
    /// (see [`ReadyState::with_shards`]); each shard decrements it once via [`signal_shard_loaded`]
    /// after its `load_shard_on_boot` returns. Load-on-boot is complete when this reaches 0.
    ///
    /// [`signal_shard_loaded`]: ReadyState::signal_shard_loaded
    load_pending: AtomicUsize,
}

impl Default for ReadyState {
    /// A zero-shard [`ReadyState`]: load-on-boot reads as ALREADY done (nothing to wait for). Used
    /// only by the unit tests / a degenerate no-shard boot; the real boot uses
    /// [`ReadyState::with_shards`].
    fn default() -> Self {
        Self::with_shards(0)
    }
}

impl ReadyState {
    /// Build readiness for a node with `shards` shards: load-on-boot is NOT done until all `shards`
    /// have signalled completion. `shards == 0` reads as already-done.
    #[must_use]
    pub fn with_shards(shards: usize) -> Self {
        ReadyState {
            load_pending: AtomicUsize::new(shards),
        }
    }

    /// Signal that ONE shard has finished its load-on-boot (its `load_shard_on_boot` returned, data
    /// loaded or a persistence-off no-op). Decrements the pending count; the LAST shard's call makes
    /// [`load_done`] flip to `true`. Saturating, so an over-signal (a wiring bug) cannot underflow.
    ///
    /// [`load_done`]: ReadyState::load_done
    pub fn signal_shard_loaded(&self) {
        // `fetch_update` with a saturating decrement: never wrap below 0 even if called more times
        // than there are shards (a defensive guard; the wiring signals exactly once per shard).
        let _ = self
            .load_pending
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                Some(n.saturating_sub(1))
            });
    }

    /// Whether load-on-boot has completed for EVERY shard (the pending count reached 0).
    #[must_use]
    pub fn load_done(&self) -> bool {
        self.load_pending.load(Ordering::SeqCst) == 0
    }
}

impl MetricsState {
    /// Build the shared metrics state. `maxmemory` is a closure over the runtime-config cell so a
    /// `CONFIG SET maxmemory` is reflected in the gauge; `raft`/`persist` are the same handles the
    /// serve layer shares (`None` outside raft-mode / with persistence off); `topology` is the
    /// `/topology` read state (#365).
    // lint-allow: a boot-wiring constructor that threads the live handles the admin HTTP endpoints
    // read; bundling these distinct boot products into a struct would just relocate the list.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        registry: MetricsRegistry,
        live: Arc<AtomicBool>,
        ready: Arc<ReadyState>,
        shards: usize,
        maxmemory: Arc<dyn Fn() -> u64 + Send + Sync>,
        raft: Option<ironcache_server::RaftHandle>,
        persist: Option<Arc<crate::persist::PersistState>>,
        topology: crate::topology::TopologyHandle,
    ) -> Self {
        MetricsState {
            registry,
            live,
            ready,
            clock: Arc::new(ClockState::new()),
            shards: shards as u64,
            maxmemory,
            raft,
            persist,
            topology,
        }
    }

    /// Assemble the process gauges for a `/metrics` scrape: uptime (Env clock), the jemalloc
    /// memory figures (one mallctl read), maxmemory, the persistence atomics, and the raft
    /// status. Each is a cheap, lock-free read off the command hot path.
    fn gauges(&self) -> MetricsGauges {
        let (used_memory, used_memory_rss) = process_memory();
        let (last_save_unix, dirty) = self
            .persist
            .as_ref()
            .map_or((0, 0), |p| (p.last_save(), p.dirty()));
        let raft = self.raft.as_ref().map(|h| {
            let s = h.status();
            RaftGauges {
                is_leader: s.is_leader(),
                current_term: s.current_term,
                commit_index: s.commit_index,
                voters: h.config().voters.len() as u64,
            }
        });
        MetricsGauges {
            uptime_secs: self.clock.uptime_secs(),
            shards: self.shards,
            used_memory,
            used_memory_rss,
            maxmemory: (self.maxmemory)(),
            last_save_unix,
            rdb_changes_since_save: dirty,
            raft,
        }
    }

    /// Whether the node is READY to serve traffic. Returns `Ok(())` when load-on-boot is done AND
    /// (raft-mode) a leader is recognized; otherwise `Err(reason)` with a short cause string the
    /// `/readyz` body reports.
    fn readiness(&self) -> Result<(), &'static str> {
        if !self.ready.load_done() {
            return Err("load-on-boot incomplete");
        }
        if let Some(raft) = &self.raft {
            // A raft node is ready only once it recognizes a leader (it has joined a formed
            // cluster / an election has resolved). `leader_id` is `Some` on a leader or a
            // follower that knows its leader; `None` while forming or mid-election.
            if raft.status().leader_id.is_none() {
                return Err("raft: no leader recognized");
            }
        }
        Ok(())
    }

    /// Render the response (status line + headers + body) for a parsed request `(method, path)`.
    /// Pure: it reads the live state and returns the bytes; the connection handler writes them.
    /// This is the routing core, exposed for tests.
    #[must_use]
    pub fn respond(&self, method: &str, path: &str) -> Vec<u8> {
        // Only GET (and HEAD, treated as GET without a body by clients) is meaningful here.
        if method != "GET" && method != "HEAD" {
            return http_response(405, "Method Not Allowed", "text/plain; charset=utf-8", b"");
        }
        // Strip any query string from the path (probes may append one).
        let path = path.split('?').next().unwrap_or(path);
        match path {
            "/metrics" => {
                // The node rollup (unchanged), then the additive per-shard labeled detail (#362):
                // `ironcache_shard_*{shard="i"}` series so the console can render shard-level views.
                let mut body =
                    ironcache_observe::render_prometheus(self.registry.aggregate(), self.gauges());
                body.push_str(&ironcache_observe::render_prometheus_shards(
                    &self.registry.per_shard_snapshots(),
                ));
                http_response(
                    200,
                    "OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    body.as_bytes(),
                )
            }
            "/topology" => {
                // Structured topology read (#365): the console reads membership/slots/epoch/raft
                // state from this JSON instead of parsing human-readable CLUSTER text. Read-only by
                // construction (it only reads the live SlotMap/RaftHandle snapshots).
                let body =
                    crate::topology::render_topology_json(&self.topology, self.raft.as_ref());
                http_response(
                    200,
                    "OK",
                    "application/json; charset=utf-8",
                    body.as_bytes(),
                )
            }
            "/livez" => {
                if self.live.load(Ordering::SeqCst) {
                    http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n")
                } else {
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        b"starting\n",
                    )
                }
            }
            "/readyz" => match self.readiness() {
                Ok(()) => http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n"),
                Err(reason) => {
                    let body = format!("not ready: {reason}\n");
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        body.as_bytes(),
                    )
                }
            },
            _ => http_response(
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            ),
        }
    }
}

/// Build a complete HTTP/1.1 response: status line, `Content-Type`, `Content-Length`,
/// `Connection: close` (we serve one request per connection), and the body.
fn http_response(code: u16, reason: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 128);
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(body);
    out
}

/// Parse the request LINE (`METHOD SP PATH SP HTTP/x.y`) from the head of the buffered request.
/// Returns `Some((method, path))` once a CRLF-terminated request line is present, `None` if the
/// line is incomplete (the caller reads more). A line with the wrong shape yields `Some` with an
/// empty method so the responder answers `400`/`405` rather than hanging.
fn parse_request_line(buf: &[u8]) -> Option<(String, String)> {
    // Find the end of the first line (CRLF or bare LF, tolerant).
    let line_end = buf.iter().position(|&b| b == b'\n')?;
    let line = &buf[..line_end];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let text = String::from_utf8_lossy(line);
    let mut parts = text.split(' ');
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();
    Some((method, path))
}

/// Serve ONE metrics connection: read (bounded by [`MAX_REQUEST_BYTES`] + the whole-request
/// [`REQUEST_DEADLINE`]) until the request line is complete, respond, flush, and close. Never reads
/// a body. Any read error / deadline / oversize closes the connection (a fixed `413` for oversize,
/// then close).
///
/// The ENTIRE request-read phase is wrapped in ONE [`REQUEST_DEADLINE`] timeout: a slow-drip client
/// that sends one byte at a time cannot reset a per-read timer to hold the socket for hours -- the
/// single deadline fires regardless of per-read progress and the connection is dropped. The write +
/// flush run AFTER the deadline (a stalled WRITER is a separate, bounded concern: best-effort and
/// the connection closes either way).
async fn serve_conn(stream: tokio::net::TcpStream, state: MetricsState) {
    serve_conn_with_deadline(stream, state, REQUEST_DEADLINE).await;
}

/// [`serve_conn`] with an explicit whole-request `deadline`, so a test can drive the slowloris-drop
/// path on a SHORT deadline instead of the production [`REQUEST_DEADLINE`]. Production always calls
/// it with the const; the deadline is the ONLY parameter.
async fn serve_conn_with_deadline(
    mut stream: tokio::net::TcpStream,
    state: MetricsState,
    deadline: Duration,
) {
    // The whole request-read phase under ONE deadline. `Err(_)` is the deadline elapsing; the inner
    // `Option` is `None` on EOF/read-error before a full request line (just close, nothing to send).
    let read_phase = tokio::time::timeout(deadline, async {
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        let mut chunk = [0u8; 1024];
        loop {
            let read = match stream.read(&mut chunk).await {
                Ok(n) if n > 0 => n,
                // EOF (0 bytes) before a full request line, or a read error: close, nothing to send.
                Ok(_) | Err(_) => return None,
            };
            buf.extend_from_slice(&chunk[..read]);
            if buf.len() > MAX_REQUEST_BYTES {
                return Some(http_response(
                    413,
                    "Payload Too Large",
                    "text/plain; charset=utf-8",
                    b"request too large\n",
                ));
            }
            if let Some((method, path)) = parse_request_line(&buf) {
                return Some(state.respond(&method, &path));
            }
            // Request line not complete yet; loop to read more (bounded by the size cap + deadline).
        }
    })
    .await;
    // `Ok(Some(resp))` is a full request line (or the 413 oversize response) within the deadline.
    // Anything else -- the deadline elapsing (a slowloris drip), or EOF/error before a request line
    // -- drops the connection with no reply.
    let Ok(Some(response)) = read_phase else {
        return;
    };
    // Best-effort write + flush; ignore errors (the client may have gone away).
    let _ = stream.write_all(&response).await;
    let _ = stream.flush().await;
}

/// The metrics ACCEPT loop: bind `addr`, then accept connections and spawn a bounded
/// [`serve_conn`] per connection on the metrics runtime. Returns when the listener errors
/// unrecoverably (a transient accept error backs off and continues).
///
/// CONCURRENCY CAP: at most [`MAX_CONCURRENT_CONNS`] connections are served at once. A
/// [`tokio::sync::Semaphore`] of that many permits gates the spawn; a permit is acquired BEFORE the
/// task spawns and held (moved into the task) for the connection's lifetime, so unbounded
/// `serve_conn` tasks cannot accumulate under a connection flood (a task-accumulation DoS). When all
/// permits are in use the EXCESS connection is dropped IMMEDIATELY (the accepted stream is closed by
/// drop) rather than queued without bound -- the accept loop never blocks, so a slow/at-capacity
/// endpoint cannot stall accepting (which would also delay observing other errors). The ops port is
/// scrape + probe traffic, so the cap is generous headroom in practice.
async fn accept_loop(listener: tokio::net::TcpListener, state: MetricsState) {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNS));
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                // Acquire a permit WITHOUT awaiting: at capacity we drop (close) the excess
                // connection instead of parking the accept loop or queueing tasks without bound.
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    // Over the concurrency cap: close this connection immediately (drop the stream).
                    drop(stream);
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    // Hold the permit for the whole connection; it is released on completion (drop).
                    let _permit = permit;
                    serve_conn(stream, state).await;
                });
            }
            Err(e) => {
                // A transient accept error (EMFILE etc.) should not kill the endpoint; back off.
                tracing::warn!(error = %e, "metrics: accept error; backing off");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Resolve `metrics_addr` (a `host:port` string) to a [`SocketAddr`]. Tries a direct parse first
/// (the common `0.0.0.0:9100` form), then a blocking DNS resolution for a hostname form. Returns
/// the FIRST resolved address.
fn resolve_metrics_addr(metrics_addr: &str) -> anyhow::Result<SocketAddr> {
    use std::net::ToSocketAddrs as _;
    if let Ok(sa) = metrics_addr.parse::<SocketAddr>() {
        return Ok(sa);
    }
    metrics_addr
        .to_socket_addrs()
        .map_err(|e| anyhow::anyhow!("resolving metrics-addr '{metrics_addr}': {e}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("metrics-addr '{metrics_addr}' resolved to no address"))
}

/// Spawn the metrics HTTP listener on its OWN OS thread + current-thread tokio runtime, bound on
/// `metrics_addr`. Returns once the listener is BOUND (a bind failure is a hard boot error, like
/// the RESP listener) so a misconfigured `--metrics-addr` fails fast rather than silently. The
/// background thread then runs the accept loop for the process lifetime; it is detached (the
/// process exit tears it down), matching the orchestrator-friendly posture.
///
/// Default-off: this is called ONLY when `--metrics-addr` is set, so with no flag no thread is
/// spawned and no socket is bound (byte-identical boot).
pub fn spawn_metrics_server(metrics_addr: &str, state: MetricsState) -> anyhow::Result<()> {
    let addr = resolve_metrics_addr(metrics_addr)?;
    // Bind synchronously on a throwaway runtime so a bind failure surfaces HERE (fail-fast),
    // then hand the bound std listener to the background thread's runtime.
    let std_listener = std::net::TcpListener::bind(addr)
        .map_err(|e| anyhow::anyhow!("binding metrics-addr {addr}: {e}"))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| anyhow::anyhow!("metrics listener set_nonblocking: {e}"))?;
    std::thread::Builder::new()
        .name("ironcache-metrics".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!(error = %e, "metrics: failed to build runtime; endpoint disabled");
                    return;
                }
            };
            rt.block_on(async move {
                let listener = match tokio::net::TcpListener::from_std(std_listener) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(error = %e, "metrics: adopting listener failed; endpoint disabled");
                        return;
                    }
                };
                accept_loop(listener, state).await;
            });
        })
        .map_err(|e| anyhow::anyhow!("spawning the metrics thread: {e}"))?;
    tracing::info!(%addr, "metrics: serving /metrics, /livez, /readyz");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_observe::ShardCounters;

    fn test_state() -> (
        MetricsState,
        MetricsRegistry,
        Arc<AtomicBool>,
        Arc<ReadyState>,
    ) {
        let registry = MetricsRegistry::new(2);
        let live = Arc::new(AtomicBool::new(false));
        // Two shards: load-on-boot is not done until BOTH have signalled.
        let ready = Arc::new(ReadyState::with_shards(2));
        let state = MetricsState::new(
            registry.clone(),
            Arc::clone(&live),
            Arc::clone(&ready),
            2,
            Arc::new(|| 0),
            None,
            None,
            crate::topology::TopologyHandle::standalone("test-node-id", 6379, 2),
        );
        (state, registry, live, ready)
    }

    #[test]
    fn metrics_route_returns_prometheus_text() {
        let (state, registry, _live, _ready) = test_state();
        // Drive two shard counters through their registry cells.
        let mut s0 = ShardCounters::with_cell(registry.shard_cell(0));
        let mut s1 = ShardCounters::with_cell(registry.shard_cell(1));
        for _ in 0..5 {
            s0.on_command();
        }
        for _ in 0..3 {
            s1.on_command();
        }
        let resp = state.respond("GET", "/metrics");
        let text = String::from_utf8(resp).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "{text}");
        assert!(
            text.contains("Content-Type: text/plain; version=0.0.4"),
            "{text}"
        );
        assert!(
            text.contains("# TYPE ironcache_commands_processed_total counter"),
            "{text}"
        );
        // 5 + 3 commands aggregated across the two shards.
        assert!(
            text.contains("ironcache_commands_processed_total 8\n"),
            "{text}"
        );
        // Per-shard labeled detail (#362): the SAME scrape carries the additive
        // `ironcache_shard_*{shard="i"}` series, here 5 on shard 0 and 3 on shard 1.
        assert!(
            text.contains("ironcache_shard_commands_processed_total{shard=\"0\"} 5\n"),
            "{text}"
        );
        assert!(
            text.contains("ironcache_shard_commands_processed_total{shard=\"1\"} 3\n"),
            "{text}"
        );
        assert!(text.contains("ironcache_uptime_seconds"), "{text}");
    }

    #[test]
    fn topology_endpoint_serves_json_with_a_coherent_standalone_answer() {
        let (state, _registry, _live, _ready) = test_state();
        let resp = String::from_utf8(state.respond("GET", "/topology")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(
            resp.contains("Content-Type: application/json"),
            "topology is served as JSON: {resp}"
        );
        let body = resp.split("\r\n\r\n").nth(1).unwrap_or("");
        // Standalone (test_state has no cluster map): node identity + a single-node slot answer.
        assert!(body.starts_with("{\"schema_version\":1,"), "{body}");
        assert!(body.contains("\"id\":\"test-node-id\""), "{body}");
        assert!(body.contains("\"mode\":\"none\""), "{body}");
        assert!(
            body.contains("\"start\":0,\"end\":16383,\"owner_id\":\"test-node-id\""),
            "self owns all slots: {body}"
        );
        assert!(body.contains("\"raft\":null"), "{body}");
    }

    #[test]
    fn livez_flips_with_the_live_flag() {
        let (state, _registry, live, _ready) = test_state();
        let before = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        live.store(true, Ordering::SeqCst);
        let after = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn readyz_503_before_ready_200_after() {
        let (state, _registry, _live, ready) = test_state();
        let before = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        assert!(before.contains("load-on-boot incomplete"), "{before}");
        // Two shards: readiness flips to 200 only after BOTH have signalled their load complete.
        ready.signal_shard_loaded();
        let after_one = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(
            after_one.starts_with("HTTP/1.1 503"),
            "one of two shards loaded: still not ready -- {after_one}"
        );
        ready.signal_shard_loaded();
        let after = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn load_done_countdown_reaches_zero() {
        // The per-shard countdown: load_done is false until every shard has signalled, and a
        // defensive over-signal cannot underflow it back to "not done".
        let ready = ReadyState::with_shards(3);
        assert!(!ready.load_done(), "3 pending");
        ready.signal_shard_loaded();
        assert!(!ready.load_done(), "2 pending");
        ready.signal_shard_loaded();
        assert!(!ready.load_done(), "1 pending");
        ready.signal_shard_loaded();
        assert!(ready.load_done(), "0 pending -> done");
        // Over-signal: saturating, so it stays done (never wraps to a huge pending count).
        ready.signal_shard_loaded();
        assert!(ready.load_done(), "over-signal stays done");
    }

    #[test]
    fn zero_shard_ready_state_is_done() {
        // A degenerate zero-shard node has nothing to load: load-on-boot reads as already done.
        let ready = ReadyState::with_shards(0);
        assert!(ready.load_done());
        assert!(ReadyState::default().load_done());
    }

    #[test]
    fn unknown_path_is_404() {
        let (state, _r, _l, _rd) = test_state();
        let resp = String::from_utf8(state.respond("GET", "/nope")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"), "{resp}");
    }

    #[test]
    fn non_get_is_405() {
        let (state, _r, _l, _rd) = test_state();
        let resp = String::from_utf8(state.respond("POST", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 405"), "{resp}");
    }

    #[test]
    fn query_string_is_stripped() {
        let (state, _r, live, _rd) = test_state();
        live.store(true, Ordering::SeqCst);
        let resp = String::from_utf8(state.respond("GET", "/livez?foo=bar")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }

    #[test]
    fn request_line_parse_incomplete_then_complete() {
        // No newline yet -> None (read more).
        assert!(parse_request_line(b"GET /metrics HTTP/1.1").is_none());
        // CRLF-terminated -> parsed.
        let (m, p) = parse_request_line(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/metrics");
        // Bare LF tolerated.
        let (m, p) = parse_request_line(b"GET /livez HTTP/1.1\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/livez");
    }

    #[test]
    fn resolve_addr_parses_socketaddr() {
        let sa = resolve_metrics_addr("127.0.0.1:9111").unwrap();
        assert_eq!(sa.port(), 9111);
    }

    /// F2 slowloris: a client that connects, sends a PARTIAL request line, then STALLS (never
    /// sending the terminating newline and never closing) is dropped by the WHOLE-REQUEST deadline,
    /// NOT held until the 8 KiB cap. We run `serve_conn_with_deadline` with a short deadline and
    /// assert it RETURNS within ~the deadline (a per-read timeout would never fire here -- the
    /// client makes one tiny write then goes idle, which under a per-read timer that resets on
    /// progress would hold for hours).
    #[tokio::test]
    async fn slow_drip_request_is_dropped_within_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The CLIENT: connect, send a partial request line, then stall (hold the socket open,
        // sending nothing more). Kept alive in this task so the server side cannot see EOF.
        let client = tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /metr").await.unwrap(); // partial: no newline, far under the 8 KiB cap.
            // Stall: never send the rest, never close, just park well past the server's deadline.
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(c);
        });

        let (stream, _peer) = listener.accept().await.unwrap();
        let (state, _r, _l, _rd) = test_state();
        // A SHORT whole-request deadline so the test is fast; production uses REQUEST_DEADLINE.
        let short_deadline = Duration::from_millis(200);
        // The server-side serve must RETURN by ~the deadline. Wrap in a generous outer timeout so a
        // regression (a per-read timer that never fires on a stalled drip) FAILS the test instead of
        // hanging it. The outer bound is far below the client's 30s stall, so completing under it
        // proves the deadline -- not the client closing -- dropped the connection.
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            serve_conn_with_deadline(stream, state, short_deadline),
        )
        .await;
        assert!(
            served.is_ok(),
            "serve_conn must drop a stalled slow-drip connection at the whole-request deadline, \
             not hold it"
        );
        client.abort();
    }

    /// The deadline path still serves a COMPLETE request that arrives in time: a full request line
    /// within the deadline gets the normal response (the deadline only drops the slow path).
    #[tokio::test]
    async fn complete_request_within_deadline_is_served() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /livez HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut raw = Vec::new();
            c.read_to_end(&mut raw).await.unwrap();
            String::from_utf8_lossy(&raw).into_owned()
        });

        let (stream, _peer) = listener.accept().await.unwrap();
        let (state, _r, live, _rd) = test_state();
        live.store(true, Ordering::SeqCst);
        serve_conn_with_deadline(stream, state, Duration::from_secs(5)).await;
        let body = client.await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    }
}
