// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console REST API (`/api/*`, issue #358), finishing #355's SLOWLOG/CLIENT
//! acquisition with a stable JSON surface over the polled topology.
//!
//! This module is PURE: it takes the inputs a request needs (the latest
//! [`Topology`], the live/ready flags, the version, and the console uptime) and
//! returns an [`ApiResponse`] (an HTTP status plus a JSON body). The HTTP layer
//! ([`crate::http`]) reads those inputs from [`crate::http::ConsoleHttpState`] and
//! wraps the result in a bounded HTTP/1.1 response, so the whole-request deadline,
//! the request-size cap, and the connection-concurrency permit still apply: the
//! API does NOT bypass the bounded responder.
//!
//! ## Shapes and status codes
//!
//! Every endpoint returns `application/json`. Data endpoints return `503` with a
//! JSON `{"error":"..."}` BEFORE the first successful poll (no topology yet), and
//! `404` JSON for an unknown node address. The shapes are stable; types derive
//! `serde::Serialize` and are rendered with `serde_json`.
//!
//! ## SECURITY (#360 auth/RBAC, #369 VPN-locked exposure)
//!
//! The `/api/*` surface exposes node internals: node addresses, the slowlog argv
//! (which contains KEY NAMES), and client IPs. It is UNAUTHENTICATED today and
//! relies on the loopback default bind. It MUST move behind the auth/RBAC tier
//! (#360) and VPN-locked exposure (#369) before the console is exposed; see the
//! wiring note in [`crate::http`].
//!
//! ## Determinism (ADR-0003)
//!
//! No clock and no RNG here: the uptime and any timestamps are passed IN by the
//! caller, which reads them through the `ironcache-env` seam.

use serde::Serialize;

use crate::history::{self, HistoryError, HistorySource, TimeSeries};
use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};

/// The default history window (seconds) when `?range=` is omitted: one hour.
const DEFAULT_RANGE_SECS: u64 = 3600;
/// The maximum history window (seconds) a request may ask for: 7 days. A larger
/// `range` is CLAMPED (not rejected) so an over-eager client cannot ask the
/// console to pull an unbounded series from Prometheus.
const MAX_RANGE_SECS: u64 = 7 * 24 * 3600;
/// The default resolution (seconds) when `?step=` is omitted.
const DEFAULT_STEP_SECS: u64 = 15;
/// The minimum step (seconds): Prometheus rejects step 0, and a tiny step over a
/// wide range explodes the point count. Clamped up to this floor.
const MIN_STEP_SECS: u64 = 1;
/// A cap on the number of points per series (range/step). Prometheus itself caps
/// at 11000; we clamp the EFFECTIVE step up so range/step never exceeds this, so a
/// `range=7d&step=1` request cannot demand a 600k-point series.
const MAX_POINTS_PER_SERIES: u64 = 11_000;

/// The content type every API response carries.
pub const CONTENT_TYPE: &str = "application/json; charset=utf-8";

/// A rendered API response: the HTTP status code and the JSON body. The HTTP
/// layer turns this into a complete HTTP/1.1 response with the bounded responder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResponse {
    /// The HTTP status code (200, 404, 503).
    pub status: u16,
    /// The JSON body (already serialized).
    pub body: String,
}

impl ApiResponse {
    /// A `200 OK` with `value` serialized to JSON. Serialization of our own
    /// response types is infallible in practice; a defensive failure degrades to a
    /// `500` JSON error rather than panicking (no `unwrap` in non-test code).
    /// Public so the management layer (#361) builds its JSON responses the same way.
    #[must_use]
    pub fn ok<T: Serialize>(value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => ApiResponse { status: 200, body },
            Err(e) => ApiResponse {
                status: 500,
                body: error_body(&format!("serializing response: {e}")),
            },
        }
    }

    /// A `503 Service Unavailable` JSON error: the console has not completed its
    /// first poll, so it has no data to serve yet.
    fn not_polled() -> Self {
        ApiResponse {
            status: 503,
            body: error_body("console has not completed its first node poll yet"),
        }
    }

    /// A `404 Not Found` JSON error with `message`.
    #[must_use]
    pub fn not_found(message: &str) -> Self {
        ApiResponse {
            status: 404,
            body: error_body(message),
        }
    }

    /// A `400 Bad Request` JSON error with `message` (a malformed / disallowed
    /// query parameter or request body).
    #[must_use]
    pub fn bad_request(message: &str) -> Self {
        ApiResponse {
            status: 400,
            body: error_body(message),
        }
    }

    /// A JSON error with an explicit status code (for the history paths that map a
    /// typed error to 400/502/503, and the management layer's 502/503).
    #[must_use]
    pub fn error(status: u16, message: &str) -> Self {
        ApiResponse {
            status,
            body: error_body(message),
        }
    }
}

/// The inputs the API needs that are NOT in the topology: process-level facts the
/// caller reads from the HTTP state (and, for uptime, through the env seam).
#[derive(Debug, Clone, Copy)]
pub struct ApiContext<'a> {
    /// The build version string (`cli::BUILD_VERSION`).
    pub version: &'a str,
    /// Whether the process is live (liveness flag).
    pub live: bool,
    /// Whether the console is ready (first successful poll done).
    pub ready: bool,
    /// Console process uptime in seconds (read through the env seam by the caller).
    pub uptime_seconds: u64,
    /// Current wall-clock Unix time in seconds (read through the env seam by the
    /// caller). Used to compute the default `/api/timeseries` window (#356).
    pub now_unix: u64,
}

/// `GET /api/health` body: liveness/readiness and process facts. Served even
/// before the first poll (it does not depend on the topology).
#[derive(Debug, Clone, Serialize)]
struct HealthResponse<'a> {
    version: &'a str,
    live: bool,
    ready: bool,
    uptime_seconds: u64,
}

/// `GET /api/cluster` body: the deployment overview plus aggregate totals. For a
/// single node the totals are that node's values; the shape sums across nodes so
/// it grows to a real cluster unchanged.
#[derive(Debug, Clone, Serialize)]
struct ClusterOverview {
    mode: TopologyMode,
    nodes_total: usize,
    nodes_reachable: usize,
    last_poll_unixtime: u64,
    /// How many seconds ago the published topology was assembled (`now - last_poll`,
    /// saturating so a poll stamped slightly ahead of the request clock reads `0`).
    /// The UI raises a staleness banner past a threshold so an operator never acts on
    /// a view the poll loop stopped refreshing (#354).
    topology_age_seconds: u64,
    totals: ClusterTotals,
    /// The cluster-wide cache hit ratio `hits / (hits + misses)` over the aggregate totals, or
    /// `None` when no reads have been served (avoids a 0/0). The cache-specific headline (#357).
    hit_ratio: Option<f64>,
    /// The cache-specific cluster topology snapshot from the structured `/topology` discovery
    /// (#354/#365): committed-epoch slot map + raft state. `None` when discovery is not configured
    /// (`node_http_url` unset); coherent single-node values in standalone mode. This is the view the
    /// console exists for (the non-goal fence: Grafana cannot express the committed-epoch slot map
    /// or raft/replica topology), distinct from the generic INFO totals above.
    cluster_topology: Option<ClusterTopologySummary>,
}

/// A curated cache-specific cluster snapshot derived from the discovered [`crate::cluster::
/// ClusterTopology`] (#354): the committed epoch, membership size, the slot-ownership rollup, and
/// the raft consensus state. Distinct from the generic INFO `ClusterTotals`.
#[derive(Debug, Clone, Serialize)]
struct ClusterTopologySummary {
    /// `none` (standalone), `static`, or `raft`.
    mode: String,
    /// Whether the node booted in cluster mode.
    enabled: bool,
    /// The committed config epoch (the fence: never two owners per slot per epoch).
    committed_epoch: u64,
    /// The number of known members.
    members: usize,
    /// The total slots that have an owner (standalone is the full 16384).
    slots_assigned: u32,
    /// The number of distinct owning nodes across the slot map.
    slot_owners: usize,
    /// The raft consensus snapshot, `None` outside raft-governance mode.
    raft: Option<RaftSummary>,
}

/// The raft consensus rollup for the cluster overview (mirrors the `/topology` raft object).
#[derive(Debug, Clone, Serialize)]
struct RaftSummary {
    is_leader: bool,
    leader_id: Option<u64>,
    term: u64,
    commit_index: u64,
    voters: u64,
}

/// Aggregate numbers across the reachable nodes (the cluster-wide totals the
/// dashboard headlines). Each is summed over the nodes that reported it.
#[derive(Debug, Clone, Default, Serialize)]
struct ClusterTotals {
    keys: u64,
    used_memory: u64,
    used_memory_rss: u64,
    connected_clients: u64,
    keyspace_hits: u64,
    keyspace_misses: u64,
    evicted_keys: u64,
    expired_keys: u64,
    /// `total_commands_processed` summed across the reachable nodes (the standard
    /// INFO Stats counter). The dashboard derives ops/second client-side by
    /// differencing this counter between two polls, which is why the console
    /// exposes the raw aggregate rather than a rate. Aggregate-only, so it stays
    /// in the OPEN tier with the other cluster totals.
    commands_processed: u64,
    /// `total_connections_received` summed across the reachable nodes (the
    /// standard INFO Stats counter), exposed for the same client-side rate
    /// derivation as `commands_processed`.
    connections_received: u64,
}

/// `GET /api/nodes` element: a compact per-node summary for the node list. The
/// full per-node detail is at `GET /api/nodes/{addr}`.
#[derive(Debug, Clone, Serialize)]
struct NodeSummary {
    addr: String,
    reachable: bool,
    version: Option<String>,
    used_memory: Option<u64>,
    keys: Option<u64>,
    connected_clients: Option<u64>,
    hit_ratio: Option<f64>,
    error: Option<String>,
}

/// `GET /api/slowlog` body: the slowlog entries, shaped per node so it grows to a
/// multi-node cluster without changing the wire shape (today: one node).
#[derive(Debug, Clone, Serialize)]
struct SlowlogResponse<'a> {
    nodes: Vec<NodeSlowlog<'a>>,
}

/// One node's slowlog within [`SlowlogResponse`].
#[derive(Debug, Clone, Serialize)]
struct NodeSlowlog<'a> {
    addr: &'a str,
    entries: &'a [crate::node::SlowlogEntry],
    error: Option<&'a str>,
}

/// `GET /api/clients` body: the client list, shaped per node (today: one node).
#[derive(Debug, Clone, Serialize)]
struct ClientsResponse<'a> {
    nodes: Vec<NodeClients<'a>>,
}

/// One node's clients within [`ClientsResponse`].
#[derive(Debug, Clone, Serialize)]
struct NodeClients<'a> {
    addr: &'a str,
    clients: &'a [crate::node::ClientInfo],
    error: Option<&'a str>,
}

/// `GET /api/keyspace` body: the total key count plus a per-database breakdown.
#[derive(Debug, Clone, Serialize)]
struct KeyspaceResponse {
    total_keys: u64,
    per_db: Vec<DbKeyspace>,
}

/// One `dbN` keyspace row (from the node INFO `# Keyspace` section).
#[derive(Debug, Clone, Serialize)]
struct DbKeyspace {
    node: String,
    db: String,
    keys: u64,
    expires: u64,
}

/// `GET /api/timeseries` body (#356): the queried metric, the resolved window, and
/// the series. The window is echoed back so the client knows what was actually
/// served after the server's bounds clamping.
#[derive(Debug, Clone, Serialize)]
struct TimeseriesResponse {
    metric: String,
    start_unix: u64,
    end_unix: u64,
    step_secs: u64,
    series: Vec<TimeSeries>,
}

/// Whether `path` (already query-stripped) is in the `/api/` namespace, so the
/// HTTP layer can route it here. `/api/openapi.json` is included.
#[must_use]
pub fn is_api_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

/// Handle one API request, async-aware. `raw_path` is the request target WITH its
/// query string (e.g. `/api/timeseries?metric=...`); `topology` is the latest
/// polled topology; `history` is the configured history source (`None` when no
/// `prometheus_url` is set). The history route (`/api/timeseries`) needs async I/O
/// and the query string, so it is handled here; every OTHER route is pure and
/// delegates to [`handle`] on the query-stripped path.
///
/// This is the entry point the HTTP layer calls; [`handle`] remains the pure,
/// synchronous core for the topology-only routes (and for unit tests).
pub async fn handle_async(
    raw_path: &str,
    topology: Option<&Topology>,
    history: Option<&(dyn HistorySource + '_)>,
    ctx: &ApiContext<'_>,
) -> ApiResponse {
    let (bare, query) = match raw_path.split_once('?') {
        Some((p, q)) => (p, q),
        None => (raw_path, ""),
    };
    if bare == "/api/timeseries" {
        return handle_timeseries(query, history, ctx).await;
    }
    handle(bare, topology, ctx)
}

/// Handle one API request. `path` is the query-stripped request path; `topology`
/// is the latest polled topology (`None` before the first poll). Returns an
/// [`ApiResponse`] (status + JSON body) for the HTTP layer to frame.
///
/// Routing:
/// * `/api/health` and `/api/openapi.json` do not need a topology.
/// * `/api/timeseries` is async (history I/O) and is routed via [`handle_async`];
///   reaching it here (the sync path) returns `503` (no history wired).
/// * every OTHER `/api/*` data route returns `503` before the first poll.
#[must_use]
pub fn handle(path: &str, topology: Option<&Topology>, ctx: &ApiContext<'_>) -> ApiResponse {
    // These do not depend on a polled topology.
    match path {
        "/api/health" => return health(ctx),
        "/api/openapi.json" => {
            return ApiResponse {
                status: 200,
                body: openapi_document().to_owned(),
            };
        }
        // The history route is async (handled by `handle_async`); reaching it on
        // the sync path means no history source was wired, so report it as the
        // unconfigured case rather than a generic 503/404.
        "/api/timeseries" => {
            return ApiResponse::error(503, &HistoryError::NotConfigured.to_string());
        }
        _ => {}
    }

    // Every data route needs a topology; before the first poll, 503.
    let Some(topo) = topology else {
        return ApiResponse::not_polled();
    };

    match path {
        "/api/cluster" => cluster(topo, ctx),
        "/api/nodes" => nodes(topo),
        "/api/slowlog" => slowlog(topo),
        "/api/clients" => clients(topo),
        "/api/keyspace" => keyspace(topo),
        other => {
            // `/api/nodes/{addr}` is the one dynamic route.
            if let Some(rest) = other.strip_prefix("/api/nodes/") {
                node_detail(topo, rest)
            } else {
                ApiResponse::not_found(&format!("no such API endpoint: {other}"))
            }
        }
    }
}

/// `GET /api/health`.
fn health(ctx: &ApiContext<'_>) -> ApiResponse {
    ApiResponse::ok(&HealthResponse {
        version: ctx.version,
        live: ctx.live,
        ready: ctx.ready,
        uptime_seconds: ctx.uptime_seconds,
    })
}

/// `GET /api/cluster`.
fn cluster(topo: &Topology, ctx: &ApiContext<'_>) -> ApiResponse {
    let nodes_total = topo.nodes.len();
    let nodes_reachable = topo.nodes.iter().filter(|n| n.reachable).count();
    let mut totals = ClusterTotals::default();
    for node in &topo.nodes {
        let Some(info) = &node.info else { continue };
        totals.keys = totals.keys.saturating_add(info.total_keys.unwrap_or(0));
        totals.used_memory = totals
            .used_memory
            .saturating_add(info.used_memory.unwrap_or(0));
        totals.used_memory_rss = totals
            .used_memory_rss
            .saturating_add(info.used_memory_rss.unwrap_or(0));
        totals.connected_clients = totals
            .connected_clients
            .saturating_add(info.connected_clients.unwrap_or(0));
        totals.keyspace_hits = totals
            .keyspace_hits
            .saturating_add(info.keyspace_hits.unwrap_or(0));
        totals.keyspace_misses = totals
            .keyspace_misses
            .saturating_add(info.keyspace_misses.unwrap_or(0));
        totals.evicted_keys = totals
            .evicted_keys
            .saturating_add(info.evicted_keys.unwrap_or(0));
        totals.expired_keys = totals
            .expired_keys
            .saturating_add(info.expired_keys.unwrap_or(0));
        totals.commands_processed = totals
            .commands_processed
            .saturating_add(info.total_commands_processed.unwrap_or(0));
        totals.connections_received = totals
            .connections_received
            .saturating_add(info.total_connections_received.unwrap_or(0));
    }
    // Cluster hit ratio over the aggregate totals (None when no reads, to avoid 0/0).
    let total_reads = totals.keyspace_hits.saturating_add(totals.keyspace_misses);
    let hit_ratio = (total_reads > 0).then(|| hit_ratio_of(totals.keyspace_hits, total_reads));
    // The cache-specific cluster snapshot from the discovered topology (#354/#357).
    let cluster_topology = topo.cluster.as_ref().map(topology_summary);
    ApiResponse::ok(&ClusterOverview {
        mode: topo.mode,
        nodes_total,
        nodes_reachable,
        last_poll_unixtime: topo.fetched_unixtime,
        // Saturating: a poll stamped a hair ahead of the request clock reads age 0, never wraps.
        topology_age_seconds: ctx.now_unix.saturating_sub(topo.fetched_unixtime),
        totals,
        hit_ratio,
        cluster_topology,
    })
}

/// `hits / total_reads` as a ratio in `[0, 1]`. `total_reads` is `> 0` at the call site.
#[allow(clippy::cast_precision_loss)] // counts; f64 covers the practical hit/miss range.
fn hit_ratio_of(hits: u64, total_reads: u64) -> f64 {
    hits as f64 / total_reads as f64
}

/// Roll a discovered [`crate::cluster::ClusterTopology`] up into the curated cache-specific summary
/// (committed epoch, membership, slot-ownership rollup, raft state) for `/api/cluster`.
fn topology_summary(ct: &crate::cluster::ClusterTopology) -> ClusterTopologySummary {
    let mut slots_assigned: u32 = 0;
    let mut owners: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for range in &ct.cluster.slots {
        if let Some(owner) = &range.owner_id {
            // An inclusive `[start, end]` range covers `end - start + 1` slots. `saturating_sub`
            // guards a malformed (end < start) range in the parsed JSON from underflow-panicking.
            slots_assigned =
                slots_assigned.saturating_add(u32::from(range.end.saturating_sub(range.start)) + 1);
            owners.insert(owner.as_str());
        }
    }
    ClusterTopologySummary {
        mode: ct.cluster.mode.clone(),
        enabled: ct.cluster.enabled,
        committed_epoch: ct.cluster.committed_epoch,
        members: ct.cluster.members.len(),
        slots_assigned,
        slot_owners: owners.len(),
        raft: ct.raft.as_ref().map(|r| RaftSummary {
            is_leader: r.is_leader,
            leader_id: r.leader_id,
            term: r.term,
            commit_index: r.commit_index,
            voters: r.voters,
        }),
    }
}

/// `GET /api/nodes`.
fn nodes(topo: &Topology) -> ApiResponse {
    let summaries: Vec<NodeSummary> = topo.nodes.iter().map(node_summary).collect();
    ApiResponse::ok(&summaries)
}

/// Build the compact [`NodeSummary`] for one snapshot.
fn node_summary(node: &NodeSnapshot) -> NodeSummary {
    let info = node.info.as_ref();
    NodeSummary {
        addr: node.addr.clone(),
        reachable: node.reachable,
        version: info.and_then(|i| i.redis_version.clone()),
        used_memory: info.and_then(|i| i.used_memory),
        keys: info.and_then(|i| i.total_keys),
        connected_clients: info.and_then(|i| i.connected_clients),
        hit_ratio: info.and_then(crate::info::NodeInfo::hit_ratio),
        error: node.error.clone(),
    }
}

/// `GET /api/nodes/{addr}` (the URL-decoded addr is passed in).
fn node_detail(topo: &Topology, raw_addr: &str) -> ApiResponse {
    let addr = percent_decode(raw_addr);
    match topo.nodes.iter().find(|n| n.addr == addr) {
        Some(node) => ApiResponse::ok(node),
        None => ApiResponse::not_found(&format!("no node with address {addr}")),
    }
}

/// `GET /api/slowlog`.
fn slowlog(topo: &Topology) -> ApiResponse {
    let nodes: Vec<NodeSlowlog<'_>> = topo
        .nodes
        .iter()
        .map(|n| NodeSlowlog {
            addr: &n.addr,
            entries: &n.slowlog,
            error: n.slowlog_error.as_deref(),
        })
        .collect();
    ApiResponse::ok(&SlowlogResponse { nodes })
}

/// `GET /api/clients`.
fn clients(topo: &Topology) -> ApiResponse {
    let nodes: Vec<NodeClients<'_>> = topo
        .nodes
        .iter()
        .map(|n| NodeClients {
            addr: &n.addr,
            clients: &n.clients,
            error: n.clients_error.as_deref(),
        })
        .collect();
    ApiResponse::ok(&ClientsResponse { nodes })
}

/// `GET /api/keyspace`. Reads the per-`dbN` rows out of each reachable node's INFO
/// raw map (`dbN:keys=<k>,expires=<e>,...`) and sums the total.
fn keyspace(topo: &Topology) -> ApiResponse {
    let mut per_db = Vec::new();
    let mut total_keys = 0u64;
    for node in &topo.nodes {
        let Some(info) = &node.info else { continue };
        // Iterate the raw map for `dbN` rows; sort by db name for stable output. A
        // row is a `db` prefix followed by a non-empty all-digit index.
        let mut rows: Vec<(&String, &String)> = info
            .raw
            .iter()
            .filter(|(k, _)| {
                k.strip_prefix("db")
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(b.0));
        for (db, value) in rows {
            let keys = field_in(value, "keys").unwrap_or(0);
            let expires = field_in(value, "expires").unwrap_or(0);
            total_keys = total_keys.saturating_add(keys);
            per_db.push(DbKeyspace {
                node: node.addr.clone(),
                db: db.clone(),
                keys,
                expires,
            });
        }
    }
    ApiResponse::ok(&KeyspaceResponse { total_keys, per_db })
}

/// `GET /api/timeseries?metric=<name>&range=<seconds>&step=<seconds>` (#356).
///
/// SECURITY: this is the SSRF / PromQL-injection boundary. The Prometheus base URL
/// is NEVER taken from the request (it comes only from server config, captured in
/// `history`). The `metric` param is ALLOWLISTED to a bare `ironcache_*` /
/// `ironcache_console_*` name ([`crate::history::is_allowed_metric`]); a name with
/// PromQL syntax (functions, label matchers, `&query=` injection) is rejected with
/// `400`. The console builds the PromQL itself from that bare name; raw PromQL
/// never crosses the boundary. The `range`/`step` are parsed and CLAMPED to bounds
/// so a request cannot demand an unbounded series.
///
/// Status codes: `503` when no source is configured; `400` for a missing /
/// disallowed `metric` or an unparseable numeric param; `502` for a source /
/// transport error; `200` with the series otherwise.
///
/// `query` is the RAW query string (the part after `?`, e.g.
/// `metric=ironcache_x&range=600`). The HTTP layer calls this directly (without
/// holding the topology lock, since the history I/O must not block the poll loop);
/// [`handle_async`] also routes here.
pub async fn handle_timeseries(
    query: &str,
    history: Option<&(dyn HistorySource + '_)>,
    ctx: &ApiContext<'_>,
) -> ApiResponse {
    // No history source configured (no prometheus_url) -> 503.
    let Some(source) = history else {
        return ApiResponse::error(503, &HistoryError::NotConfigured.to_string());
    };

    // Parse the query parameters.
    let params = QueryParams::parse(query);
    let Some(metric) = params.get("metric") else {
        return ApiResponse::bad_request("missing required query parameter 'metric'");
    };
    // SECURITY: allowlist the metric to a bare ironcache_* name BEFORE it is used.
    if !history::is_allowed_metric(&metric) {
        return ApiResponse::bad_request(&format!(
            "metric '{metric}' is not an allowed ironcache_* / ironcache_console_* metric name"
        ));
    }
    // range / step: parse (a present-but-unparseable value is a 400), default, and
    // clamp to the bounds.
    let range = match params.get_u64("range") {
        Ok(v) => v.unwrap_or(DEFAULT_RANGE_SECS),
        Err(()) => return ApiResponse::bad_request("query parameter 'range' must be an integer"),
    };
    let step = match params.get_u64("step") {
        Ok(v) => v.unwrap_or(DEFAULT_STEP_SECS),
        Err(()) => return ApiResponse::bad_request("query parameter 'step' must be an integer"),
    };
    let (start_unix, end_unix, step_secs) = resolve_window(ctx.now_unix, range, step);

    match source
        .query_range(&metric, start_unix, end_unix, step_secs)
        .await
    {
        Ok(series) => ApiResponse::ok(&TimeseriesResponse {
            metric,
            start_unix,
            end_unix,
            step_secs,
            series,
        }),
        Err(e) => match e {
            // Defense in depth: the source re-checks the allowlist; map to 400.
            HistoryError::DisallowedMetric(_) => ApiResponse::bad_request(&e.to_string()),
            HistoryError::NotConfigured => ApiResponse::error(503, &e.to_string()),
            // A transport / source failure is an upstream (bad gateway) condition.
            HistoryError::Transport(_) | HistoryError::Source(_) | HistoryError::Parse(_) => {
                ApiResponse::error(502, &e.to_string())
            }
        },
    }
}

/// Resolve and CLAMP the history window from `now`, the requested `range`, and the
/// requested `step`. Returns `(start_unix, end_unix, step_secs)`. The window is
/// `now - range .. now`; `range` is clamped to [`MAX_RANGE_SECS`], `step` to a
/// floor of [`MIN_STEP_SECS`], and then `step` is raised further if needed so the
/// point count (`range / step`) does not exceed [`MAX_POINTS_PER_SERIES`].
fn resolve_window(now: u64, range: u64, step: u64) -> (u64, u64, u64) {
    let range = range.clamp(1, MAX_RANGE_SECS);
    let mut step = step.max(MIN_STEP_SECS);
    // Cap the resulting point count by raising the step if the client asked for a
    // tiny step over a wide range (range/step would otherwise be enormous).
    let max_points = MAX_POINTS_PER_SERIES.max(1);
    if range / step > max_points {
        // ceil(range / max_points), at least MIN_STEP_SECS.
        step = range.div_ceil(max_points).max(MIN_STEP_SECS);
    }
    let end_unix = now;
    let start_unix = now.saturating_sub(range);
    (start_unix, end_unix, step)
}

/// A tiny URL query-string parser (`a=b&c=d`), percent-decoding keys and values.
/// Hand-rolled (no url crate) to keep the dependency posture; sufficient for the
/// console's flat, small query strings. Public so the management layer (#361) can
/// parse the SCAN browser's `?pattern=&cursor=&count=` query through the same
/// decoder the history route uses.
pub struct QueryParams {
    pairs: Vec<(String, String)>,
}

impl QueryParams {
    /// Parse `a=b&c=d`. A pair without `=` is recorded with an empty value; an
    /// empty segment is skipped. `+` is decoded to a space (form-encoding), then
    /// `%XX` escapes are decoded.
    #[must_use]
    pub fn parse(query: &str) -> Self {
        let mut pairs = Vec::new();
        for seg in query.split('&') {
            if seg.is_empty() {
                continue;
            }
            let (k, v) = match seg.split_once('=') {
                Some((k, v)) => (k, v),
                None => (seg, ""),
            };
            pairs.push((query_decode(k), query_decode(v)));
        }
        QueryParams { pairs }
    }

    /// The first value for `key`, if present.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<String> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    /// Parse the value for `key` as a `u64`. `Ok(None)` when absent; `Err(())` when
    /// present but not a valid integer (so the caller returns a `400`).
    #[allow(clippy::result_unit_err)]
    pub fn get_u64(&self, key: &str) -> Result<Option<u64>, ()> {
        match self.get(key) {
            None => Ok(None),
            Some(s) => s.trim().parse::<u64>().map(Some).map_err(|_| ()),
        }
    }
}

/// Decode a URL query component: `+` -> space, then `%XX` escapes. A malformed `%`
/// escape is left verbatim (tolerant). Reuses the path percent-decoder for the
/// `%XX` pass.
fn query_decode(s: &str) -> String {
    let with_spaces = s.replace('+', " ");
    percent_decode(&with_spaces)
}

/// Extract a `name=<u64>` field from a comma-separated keyspace value
/// (`keys=10,expires=2,avg_ttl=0`).
fn field_in(value: &str, name: &str) -> Option<u64> {
    let prefix = format!("{name}=");
    value
        .split(',')
        .find_map(|part| part.trim().strip_prefix(&prefix))
        .and_then(|n| n.trim().parse::<u64>().ok())
}

/// A JSON `{"error":"<message>"}` body for callers OUTSIDE this module (the HTTP
/// auth/RBAC gate in [`crate::http`], #360, builds its 401/403 bodies with the
/// same escaped shape every other API error uses).
#[must_use]
pub fn error_json(message: &str) -> String {
    error_body(message)
}

/// A JSON `{"error":"<message>"}` body. The message is escaped via `serde_json`
/// (no hand-rolled escaping) so a message with quotes / control chars stays valid
/// JSON.
fn error_body(message: &str) -> String {
    #[derive(Serialize)]
    struct ErrorBody<'a> {
        error: &'a str,
    }
    // serde_json on a single &str field cannot realistically fail; on the
    // impossible error path, fall back to a fixed valid JSON string.
    serde_json::to_string(&ErrorBody { error: message })
        .unwrap_or_else(|_| "{\"error\":\"internal error\"}".to_owned())
}

/// Public percent-decoder for a URL path segment (#361): the management layer
/// URL-decodes the `{k}` key and the `{name}` ACL user out of the path with the
/// same tolerant decoder the node-addr route uses (`%3A` -> `:`, a malformed `%`
/// left verbatim).
#[must_use]
pub fn percent_decode_path(s: &str) -> String {
    percent_decode(s)
}

/// Minimal percent-decoding for a path segment: turns `%3A` into `:` etc. The
/// node addr contains a colon, which a client may send percent-encoded (`%3A`).
/// Tolerant: a malformed `%` escape is left verbatim rather than erroring. This
/// is sufficient for `host:port` addresses; it is NOT a general URL decoder.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_val(bytes[i + 1]);
            let lo = hex_val(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Hex digit value for one ASCII byte, or `None` if it is not a hex digit.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// The hand-written OpenAPI 3.0 document for the `/api/*` surface, served as a
/// static string at `GET /api/openapi.json`. Concise but VALID: it parses as JSON
/// and carries the required `openapi`/`info`/`paths` keys with the response
/// schemas. Kept as a string literal (no codegen) to stay dependency-light.
#[must_use]
pub fn openapi_document() -> &'static str {
    OPENAPI_JSON
}

/// The static OpenAPI 3.0 JSON. Validity is asserted by a unit test that parses
/// it and checks the top-level keys and every documented path.
const OPENAPI_JSON: &str = r##"{
  "openapi": "3.0.3",
  "info": {
    "title": "IronCache Console API",
    "version": "1.1.0",
    "description": "Monitoring + node-level management API over a polled IronCache deployment. The read surface is gated by a three-tier RBAC (#360); the management writes (#361, CONFIG SET, key CRUD, the command console, pub/sub publish, ACL user management, persistence save) are ADMIN-tier and enforced server-side. To be placed behind the VPN-locked exposure (#369)."
  },
  "paths": {
    "/api/health": {
      "get": {
        "summary": "Liveness, readiness, version, and uptime.",
        "responses": {
          "200": {
            "description": "Health.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Health" }
              }
            }
          }
        }
      }
    },
    "/api/cluster": {
      "get": {
        "summary": "Cluster overview and aggregate totals.",
        "responses": {
          "200": {
            "description": "Overview.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/ClusterOverview" }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/nodes": {
      "get": {
        "summary": "Per-node summaries.",
        "responses": {
          "200": {
            "description": "Node summaries.",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": { "$ref": "#/components/schemas/NodeSummary" }
                }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/nodes/{addr}": {
      "get": {
        "summary": "Full snapshot for one node (addr is host:port, URL-encoded).",
        "parameters": [
          {
            "name": "addr",
            "in": "path",
            "required": true,
            "schema": { "type": "string" }
          }
        ],
        "responses": {
          "200": {
            "description": "Node snapshot.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/NodeSnapshot" }
              }
            }
          },
          "404": {
            "description": "No such node.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/slowlog": {
      "get": {
        "summary": "Slowlog entries per node.",
        "responses": {
          "200": {
            "description": "Slowlog.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/SlowlogResponse" }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/clients": {
      "get": {
        "summary": "Connected clients per node.",
        "responses": {
          "200": {
            "description": "Clients.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/ClientsResponse" }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/keyspace": {
      "get": {
        "summary": "Total key count and per-database breakdown.",
        "responses": {
          "200": {
            "description": "Keyspace.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/KeyspaceResponse" }
              }
            }
          },
          "503": {
            "description": "No poll completed yet.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/timeseries": {
      "get": {
        "summary": "Historical time series for an allowlisted ironcache_* metric (via Prometheus).",
        "parameters": [
          {
            "name": "metric",
            "in": "query",
            "required": true,
            "description": "A bare ironcache_* / ironcache_console_* metric name (allowlisted; raw PromQL is rejected).",
            "schema": { "type": "string" }
          },
          {
            "name": "range",
            "in": "query",
            "required": false,
            "description": "Window length in seconds back from now (default 3600, clamped to 7 days).",
            "schema": { "type": "integer", "format": "int64" }
          },
          {
            "name": "step",
            "in": "query",
            "required": false,
            "description": "Resolution in seconds (default 15, raised if range/step would exceed 11000 points).",
            "schema": { "type": "integer", "format": "int64" }
          }
        ],
        "responses": {
          "200": {
            "description": "The series.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/TimeseriesResponse" }
              }
            }
          },
          "400": {
            "description": "Missing or disallowed metric, or a bad numeric parameter.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          },
          "502": {
            "description": "The history source (Prometheus) failed.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          },
          "503": {
            "description": "No history source configured.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/Error" }
              }
            }
          }
        }
      }
    },
    "/api/openapi.json": {
      "get": {
        "summary": "This OpenAPI document.",
        "responses": {
          "200": {
            "description": "The OpenAPI 3.0 document."
          }
        }
      }
    },
    "/api/config": {
      "get": {
        "summary": "All CONFIG GET parameters (PRIVILEGED_READ).",
        "responses": {
          "200": {
            "description": "Sorted parameter list.",
            "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConfigResponse" } } }
          },
          "401": { "description": "Auth required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "503": { "description": "No seed node configured.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      },
      "post": {
        "summary": "CONFIG SET a parameter (ADMIN).",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConfigSetBody" } } } },
        "responses": {
          "200": { "description": "Applied.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Invalid param or body.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "401": { "description": "Auth required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node rejected the set.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/keys": {
      "get": {
        "summary": "SCAN the keyspace, one bounded page (PRIVILEGED_READ).",
        "parameters": [
          { "name": "pattern", "in": "query", "required": false, "description": "MATCH glob (default *).", "schema": { "type": "string" } },
          { "name": "cursor", "in": "query", "required": false, "description": "SCAN cursor (default 0).", "schema": { "type": "string" } },
          { "name": "count", "in": "query", "required": false, "description": "COUNT hint (default 100, clamped).", "schema": { "type": "integer", "format": "int64" } }
        ],
        "responses": {
          "200": { "description": "A SCAN page.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ScanResponse" } } } },
          "400": { "description": "Pattern too long.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/keys/{k}": {
      "parameters": [ { "name": "k", "in": "path", "required": true, "description": "URL-encoded key name.", "schema": { "type": "string" } } ],
      "get": {
        "summary": "Inspect one key: type, TTL, and a bounded value (PRIVILEGED_READ).",
        "responses": {
          "200": { "description": "Key detail.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/KeyDetail" } } } },
          "404": { "description": "No such key.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      },
      "post": {
        "summary": "SET a string value on the key (ADMIN). Typed writes are a string SET in v1.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/KeySetBody" } } } },
        "responses": {
          "200": { "description": "Set.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Bad key or value.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      },
      "delete": {
        "summary": "DEL the key (ADMIN).",
        "responses": {
          "200": { "description": "Deleted count.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Deleted" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/keys/{k}/expire": {
      "parameters": [ { "name": "k", "in": "path", "required": true, "schema": { "type": "string" } } ],
      "post": {
        "summary": "EXPIRE the key in N seconds (ADMIN).",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ExpireBody" } } } },
        "responses": {
          "200": { "description": "Whether the timeout was set.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Negative seconds.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/keys/{k}/persist": {
      "parameters": [ { "name": "k", "in": "path", "required": true, "schema": { "type": "string" } } ],
      "post": {
        "summary": "PERSIST the key (remove its TTL) (ADMIN).",
        "responses": {
          "200": { "description": "Whether a TTL was removed.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } }
        }
      }
    },
    "/api/command": {
      "post": {
        "summary": "Run an arbitrary command over the node RESP connection (ADMIN). Bounded argv; the node ACL is the bound.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/CommandBody" } } } },
        "responses": {
          "200": { "description": "The rendered reply (a node -ERR is reply.kind=error).", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/CommandResponse" } } } },
          "400": { "description": "Empty / over-bounds argv.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/pubsub/channels": {
      "get": {
        "summary": "Active pub/sub channels and subscriber counts (PRIVILEGED_READ).",
        "responses": {
          "200": { "description": "Channel list.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ChannelsResponse" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/pubsub/publish": {
      "post": {
        "summary": "PUBLISH a message to a channel (ADMIN).",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PublishBody" } } } },
        "responses": {
          "200": { "description": "Receiver count.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PublishResponse" } } } },
          "400": { "description": "Empty channel / over-long message.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/cluster/rebalance-plan": {
      "get": {
        "summary": "Cluster rebalance DRY-RUN plan: per-node current vs balanced target slots + signed move (ADMIN, read-only). #361 over engine CLUSTER REBALANCE DRYRUN.",
        "responses": {
          "200": { "description": "The per-node plan + rollup (dry_run is always true; the engine refuses APPLY).", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RebalancePlanResponse" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error, cluster support disabled, or an unexpected reply.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/cluster/failover": {
      "post": {
        "summary": "Trigger a bare CLUSTER FAILOVER (ADMIN, MUTATING). Engine-gated to an in-sync replica; FORCE/TAKEOVER not offered. Requires confirm=FAILOVER. #361.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/FailoverBody" } } } },
        "responses": {
          "200": { "description": "The failover was proposed/accepted by the node.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Missing / wrong confirmation token.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "The node refused the failover (e.g. not an in-sync replica) or is unreachable.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/cluster/meet": {
      "post": {
        "summary": "Add a node to the cluster: CLUSTER MEET host port (ADMIN, additive). #361.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MeetBody" } } } },
        "responses": {
          "200": { "description": "The node was added / the handshake was accepted.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Empty/CRLF host or a zero port.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "The node rejected the address or is unreachable.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/cluster/forget": {
      "post": {
        "summary": "Remove a node: CLUSTER FORGET node-id (ADMIN, DESTRUCTIVE). confirm must echo node_id. #361.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ForgetBody" } } } },
        "responses": {
          "200": { "description": "The node was forgotten.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Empty/CRLF node_id or confirm does not echo it.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "The node refused the forget (unknown id / self) or is unreachable.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/cluster/setslot": {
      "post": {
        "summary": "Online-migration / slot FLIP: CLUSTER SETSLOT slot {NODE|MIGRATING|IMPORTING} node-id, or slot STABLE (ADMIN, DESTRUCTIVE). confirm must echo the slot. #361.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SetslotBody" } } } },
        "responses": {
          "200": { "description": "The slot transition was applied.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Out-of-range slot, unknown action, missing node_id, or confirm does not echo the slot.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "The node refused the transition (unknown node, etc.) or is unreachable.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/acl": {
      "get": {
        "summary": "ACL WHOAMI + LIST + CAT (ADMIN: the full user/permission set).",
        "responses": {
          "200": { "description": "ACL state.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/AclResponse" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/acl/user": {
      "post": {
        "summary": "ACL SETUSER username rules... (ADMIN).",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/AclUserBody" } } } },
        "responses": {
          "200": { "description": "Set.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "400": { "description": "Bad username / rule.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/acl/user/{name}": {
      "parameters": [ { "name": "name", "in": "path", "required": true, "schema": { "type": "string" } } ],
      "delete": {
        "summary": "ACL DELUSER name (ADMIN).",
        "responses": {
          "200": { "description": "Deleted count.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Deleted" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/persistence": {
      "get": {
        "summary": "Persistence facts from INFO persistence (PRIVILEGED_READ).",
        "responses": {
          "200": { "description": "Persistence state.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PersistenceResponse" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/api/persistence/save": {
      "post": {
        "summary": "BGSAVE (default) or SAVE (ADMIN).",
        "requestBody": { "required": false, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SaveBody" } } } },
        "responses": {
          "200": { "description": "Save started.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Ok" } } } },
          "403": { "description": "Admin tier required.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "502": { "description": "Node error.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "Error": {
        "type": "object",
        "properties": { "error": { "type": "string" } },
        "required": ["error"]
      },
      "Health": {
        "type": "object",
        "properties": {
          "version": { "type": "string" },
          "live": { "type": "boolean" },
          "ready": { "type": "boolean" },
          "uptime_seconds": { "type": "integer", "format": "int64" }
        },
        "required": ["version", "live", "ready", "uptime_seconds"]
      },
      "ClusterTotals": {
        "type": "object",
        "properties": {
          "keys": { "type": "integer", "format": "int64" },
          "used_memory": { "type": "integer", "format": "int64" },
          "used_memory_rss": { "type": "integer", "format": "int64" },
          "connected_clients": { "type": "integer", "format": "int64" },
          "keyspace_hits": { "type": "integer", "format": "int64" },
          "keyspace_misses": { "type": "integer", "format": "int64" },
          "evicted_keys": { "type": "integer", "format": "int64" },
          "expired_keys": { "type": "integer", "format": "int64" },
          "commands_processed": { "type": "integer", "format": "int64" },
          "connections_received": { "type": "integer", "format": "int64" }
        }
      },
      "ClusterOverview": {
        "type": "object",
        "properties": {
          "mode": { "type": "string", "enum": ["standalone", "clustered"] },
          "nodes_total": { "type": "integer" },
          "nodes_reachable": { "type": "integer" },
          "last_poll_unixtime": { "type": "integer", "format": "int64" },
          "topology_age_seconds": { "type": "integer", "format": "int64" },
          "totals": { "$ref": "#/components/schemas/ClusterTotals" }
        }
      },
      "NodeSummary": {
        "type": "object",
        "properties": {
          "addr": { "type": "string" },
          "reachable": { "type": "boolean" },
          "version": { "type": "string", "nullable": true },
          "used_memory": { "type": "integer", "format": "int64", "nullable": true },
          "keys": { "type": "integer", "format": "int64", "nullable": true },
          "connected_clients": { "type": "integer", "format": "int64", "nullable": true },
          "hit_ratio": { "type": "number", "format": "double", "nullable": true },
          "error": { "type": "string", "nullable": true }
        }
      },
      "NodeInfo": {
        "type": "object",
        "additionalProperties": true
      },
      "SlowlogEntry": {
        "type": "object",
        "properties": {
          "id": { "type": "integer", "format": "int64" },
          "timestamp": { "type": "integer", "format": "int64" },
          "micros": { "type": "integer", "format": "int64" },
          "argv": { "type": "array", "items": { "type": "string" } },
          "client_addr": { "type": "string" },
          "client_name": { "type": "string" }
        }
      },
      "ClientInfo": {
        "type": "object",
        "additionalProperties": true
      },
      "NodeSnapshot": {
        "type": "object",
        "properties": {
          "addr": { "type": "string" },
          "reachable": { "type": "boolean" },
          "error": { "type": "string", "nullable": true },
          "info": { "$ref": "#/components/schemas/NodeInfo" },
          "slowlog": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/SlowlogEntry" }
          },
          "slowlog_error": { "type": "string", "nullable": true },
          "clients": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/ClientInfo" }
          },
          "clients_error": { "type": "string", "nullable": true },
          "fetched_unixtime": { "type": "integer", "format": "int64" }
        }
      },
      "SlowlogResponse": {
        "type": "object",
        "properties": {
          "nodes": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "addr": { "type": "string" },
                "entries": {
                  "type": "array",
                  "items": { "$ref": "#/components/schemas/SlowlogEntry" }
                },
                "error": { "type": "string", "nullable": true }
              }
            }
          }
        }
      },
      "ClientsResponse": {
        "type": "object",
        "properties": {
          "nodes": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "addr": { "type": "string" },
                "clients": {
                  "type": "array",
                  "items": { "$ref": "#/components/schemas/ClientInfo" }
                },
                "error": { "type": "string", "nullable": true }
              }
            }
          }
        }
      },
      "KeyspaceResponse": {
        "type": "object",
        "properties": {
          "total_keys": { "type": "integer", "format": "int64" },
          "per_db": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "node": { "type": "string" },
                "db": { "type": "string" },
                "keys": { "type": "integer", "format": "int64" },
                "expires": { "type": "integer", "format": "int64" }
              }
            }
          }
        }
      },
      "TimeSeries": {
        "type": "object",
        "properties": {
          "labels": {
            "type": "object",
            "additionalProperties": { "type": "string" }
          },
          "points": {
            "type": "array",
            "description": "Samples as [unix_ts_seconds, value] pairs in time order.",
            "items": {
              "type": "array",
              "items": { "type": "number" },
              "minItems": 2,
              "maxItems": 2
            }
          }
        }
      },
      "TimeseriesResponse": {
        "type": "object",
        "properties": {
          "metric": { "type": "string" },
          "start_unix": { "type": "integer", "format": "int64" },
          "end_unix": { "type": "integer", "format": "int64" },
          "step_secs": { "type": "integer", "format": "int64" },
          "series": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/TimeSeries" }
          }
        }
      },
      "Ok": {
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"]
      },
      "Deleted": {
        "type": "object",
        "properties": { "deleted": { "type": "integer", "format": "int64" } },
        "required": ["deleted"]
      },
      "ConfigSetBody": {
        "type": "object",
        "properties": {
          "param": { "type": "string", "description": "A config parameter token (no whitespace/CRLF)." },
          "value": { "type": "string" }
        },
        "required": ["param", "value"]
      },
      "ConfigResponse": {
        "type": "object",
        "properties": {
          "params": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": { "param": { "type": "string" }, "value": { "type": "string" } }
            }
          }
        }
      },
      "ScanKey": {
        "type": "object",
        "properties": {
          "key": { "type": "string" },
          "type": { "type": "string" },
          "ttl": { "type": "integer", "format": "int64", "description": "-1 no expiry, -2 missing." }
        }
      },
      "ScanResponse": {
        "type": "object",
        "properties": {
          "cursor": { "type": "string", "description": "Next SCAN cursor (0 when complete)." },
          "keys": { "type": "array", "items": { "$ref": "#/components/schemas/ScanKey" } }
        }
      },
      "KeyValue": {
        "type": "object",
        "description": "Tagged by kind: string|elements|pairs|none.",
        "properties": {
          "kind": { "type": "string", "enum": ["string", "elements", "pairs", "none"] },
          "data": { "type": "string" },
          "items": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["kind"]
      },
      "KeyDetail": {
        "type": "object",
        "properties": {
          "key": { "type": "string" },
          "type": { "type": "string" },
          "ttl": { "type": "integer", "format": "int64" },
          "value": { "$ref": "#/components/schemas/KeyValue" },
          "truncated": { "type": "boolean" }
        }
      },
      "KeySetBody": {
        "type": "object",
        "properties": { "value": { "type": "string" } },
        "required": ["value"]
      },
      "ExpireBody": {
        "type": "object",
        "properties": { "seconds": { "type": "integer", "format": "int64", "description": "A non-negative TTL." } },
        "required": ["seconds"]
      },
      "CommandBody": {
        "type": "object",
        "properties": { "args": { "type": "array", "items": { "type": "string" }, "description": "Non-empty argv; each arg and the total are bounded." } },
        "required": ["args"]
      },
      "RenderedReply": {
        "type": "object",
        "description": "Tagged by kind: simple|error|integer|bulk|array.",
        "properties": {
          "kind": { "type": "string", "enum": ["simple", "error", "integer", "bulk", "array"] },
          "value": {},
          "items": { "type": "array", "items": { "$ref": "#/components/schemas/RenderedReply" } }
        },
        "required": ["kind"]
      },
      "CommandResponse": {
        "type": "object",
        "properties": {
          "ok": { "type": "boolean" },
          "command": { "type": "string" },
          "reply": { "$ref": "#/components/schemas/RenderedReply" }
        }
      },
      "FailoverBody": {
        "type": "object",
        "properties": { "confirm": { "type": "string", "description": "Must equal \"FAILOVER\"." } },
        "required": ["confirm"]
      },
      "MeetBody": {
        "type": "object",
        "properties": {
          "host": { "type": "string" },
          "port": { "type": "integer", "format": "int32", "minimum": 1, "maximum": 65535 }
        },
        "required": ["host", "port"]
      },
      "ForgetBody": {
        "type": "object",
        "properties": {
          "node_id": { "type": "string" },
          "confirm": { "type": "string", "description": "Must echo node_id." }
        },
        "required": ["node_id", "confirm"]
      },
      "SetslotBody": {
        "type": "object",
        "properties": {
          "slot": { "type": "integer", "format": "int32", "minimum": 0, "maximum": 16383 },
          "action": { "type": "string", "enum": ["NODE", "MIGRATING", "IMPORTING", "STABLE"] },
          "node_id": { "type": "string", "nullable": true, "description": "Required except for STABLE." },
          "confirm": { "type": "string", "description": "Must echo the slot number." }
        },
        "required": ["slot", "action", "confirm"]
      },
      "RebalancePlanResponse": {
        "type": "object",
        "properties": {
          "ok": { "type": "boolean" },
          "dry_run": { "type": "boolean" },
          "balanced": { "type": "boolean" },
          "total_slots_to_move": { "type": "integer", "format": "int64" },
          "targets": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "node": { "type": "string" },
                "current_slots": { "type": "integer", "format": "int64" },
                "target_slots": { "type": "integer", "format": "int64" },
                "slots_to_move": { "type": "integer", "format": "int64" }
              }
            }
          }
        }
      },
      "ChannelsResponse": {
        "type": "object",
        "properties": {
          "channels": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": { "channel": { "type": "string" }, "subs": { "type": "integer", "format": "int64" } }
            }
          }
        }
      },
      "PublishBody": {
        "type": "object",
        "properties": { "channel": { "type": "string" }, "message": { "type": "string" } },
        "required": ["channel", "message"]
      },
      "PublishResponse": {
        "type": "object",
        "properties": { "receivers": { "type": "integer", "format": "int64" } }
      },
      "AclResponse": {
        "type": "object",
        "properties": {
          "whoami": { "type": "string" },
          "users": { "type": "array", "items": { "type": "string" } },
          "categories": { "type": "array", "items": { "type": "string" } }
        }
      },
      "AclUserBody": {
        "type": "object",
        "properties": {
          "username": { "type": "string" },
          "rules": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["username"]
      },
      "PersistenceResponse": {
        "type": "object",
        "properties": {
          "last_save_unixtime": { "type": "integer", "format": "int64", "nullable": true },
          "changes_since_save": { "type": "integer", "format": "int64", "nullable": true },
          "rdb_enabled": { "type": "boolean" },
          "aof_enabled": { "type": "boolean" },
          "last_bgsave_status": { "type": "string", "nullable": true }
        }
      },
      "SaveBody": {
        "type": "object",
        "properties": { "background": { "type": "boolean", "description": "BGSAVE (true) vs blocking SAVE (false)." } }
      }
    }
  }
}"##;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::NodeInfo;
    use crate::node::{ClientInfo, SlowlogEntry};
    use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};

    fn ctx() -> ApiContext<'static> {
        ApiContext {
            version: "test-1.0",
            live: true,
            ready: true,
            uptime_seconds: 7,
            now_unix: 1_700_000_000,
        }
    }

    fn sample_node() -> NodeSnapshot {
        let mut raw = std::collections::HashMap::new();
        raw.insert("db0".to_owned(), "keys=10,expires=2,avg_ttl=0".to_owned());
        raw.insert("db1".to_owned(), "keys=5,expires=0,avg_ttl=0".to_owned());
        let info = NodeInfo {
            redis_version: Some("7.2.4".to_owned()),
            connected_clients: Some(3),
            used_memory: Some(1024),
            used_memory_rss: Some(2048),
            keyspace_hits: Some(80),
            keyspace_misses: Some(20),
            evicted_keys: Some(1),
            expired_keys: Some(4),
            total_commands_processed: Some(5000),
            total_connections_received: Some(120),
            total_keys: Some(15),
            cluster_enabled: false,
            raw,
            ..Default::default()
        };
        NodeSnapshot {
            addr: "10.0.0.1:6379".to_owned(),
            reachable: true,
            error: None,
            info: Some(info),
            slowlog: vec![SlowlogEntry {
                id: 1,
                timestamp: 1_700_000_000,
                micros: 12_000,
                argv: vec!["GET".to_owned(), "foo".to_owned()],
                client_addr: "10.0.0.7:5000".to_owned(),
                client_name: "w1".to_owned(),
            }],
            slowlog_error: None,
            clients: vec![ClientInfo {
                id: Some(7),
                addr: Some("127.0.0.1:6379".to_owned()),
                ..Default::default()
            }],
            clients_error: None,
            fetched_unixtime: 1_700_000_001,
        }
    }

    fn topo() -> Topology {
        Topology {
            mode: TopologyMode::Standalone,
            nodes: vec![sample_node()],
            cluster: None,
            fetched_unixtime: 1_700_000_001,
        }
    }

    /// Parse an `ApiResponse` body as JSON, asserting it parsed.
    fn parse(resp: &ApiResponse) -> serde_json::Value {
        serde_json::from_str(&resp.body).expect("API body must be valid JSON")
    }

    #[test]
    fn health_works_without_a_topology() {
        let resp = handle("/api/health", None, &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["version"], "test-1.0");
        assert_eq!(v["live"], true);
        assert_eq!(v["ready"], true);
        assert_eq!(v["uptime_seconds"], 7);
    }

    #[test]
    fn data_routes_are_503_before_first_poll() {
        for path in [
            "/api/cluster",
            "/api/nodes",
            "/api/nodes/10.0.0.1:6379",
            "/api/slowlog",
            "/api/clients",
            "/api/keyspace",
        ] {
            let resp = handle(path, None, &ctx());
            assert_eq!(resp.status, 503, "{path} should be 503 before first poll");
            let v = parse(&resp);
            assert!(v["error"].is_string(), "{path}: {}", resp.body);
        }
    }

    #[test]
    fn cluster_overview_sums_totals_for_single_node() {
        let t = topo();
        let resp = handle("/api/cluster", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["mode"], "standalone");
        assert_eq!(v["nodes_total"], 1);
        assert_eq!(v["nodes_reachable"], 1);
        assert_eq!(v["last_poll_unixtime"], 1_700_000_001u64);
        // The fixture stamps the poll 1s AHEAD of ctx.now_unix, so the saturating age is 0
        // (a poll can never read as "negative age" / a huge wrapped value).
        assert_eq!(v["topology_age_seconds"], 0u64);
        assert_eq!(v["totals"]["keys"], 15);
        assert_eq!(v["totals"]["used_memory"], 1024);
        assert_eq!(v["totals"]["connected_clients"], 3);
        assert_eq!(v["totals"]["keyspace_hits"], 80);
        assert_eq!(v["totals"]["commands_processed"], 5000);
        assert_eq!(v["totals"]["connections_received"], 120);
        // Cluster hit ratio over the totals: 80 / (80 + 20) = 0.8 (#357).
        assert!((v["hit_ratio"].as_f64().unwrap() - 0.8).abs() < 1e-9);
        // No /topology discovery configured in this fixture, so the cache-specific summary is null.
        assert!(v["cluster_topology"].is_null());
    }

    #[test]
    fn cluster_overview_reports_topology_age_when_now_is_ahead_of_the_poll() {
        // A request whose now_unix is 30s after the topology was stamped: age is 30.
        let t = topo(); // fetched_unixtime = 1_700_000_001
        let aged_ctx = ApiContext {
            now_unix: 1_700_000_031,
            ..ctx()
        };
        let resp = handle("/api/cluster", Some(&t), &aged_ctx);
        let v = parse(&resp);
        assert_eq!(
            v["topology_age_seconds"], 30u64,
            "age is now_unix - last_poll_unixtime"
        );
    }

    #[test]
    fn cluster_overview_includes_the_discovered_topology_summary() {
        use crate::cluster::{
            ClusterTopology, TopoClusterView, TopoMember, TopoNode, TopoRaft, TopoReplication,
            TopoSlotRange,
        };
        let mut t = topo();
        // Attach a discovered 2-node raft topology with the slot space split between the owners.
        t.cluster = Some(ClusterTopology {
            schema_version: 1,
            node: TopoNode {
                id: "n1".into(),
                engine_version: "v".into(),
                tcp_port: 7000,
                shards: 1,
            },
            cluster: TopoClusterView {
                mode: "raft".into(),
                enabled: true,
                committed_epoch: 9,
                members: vec![
                    TopoMember {
                        id: "n1".into(),
                        host: "10.0.0.1".into(),
                        port: 7000,
                    },
                    TopoMember {
                        id: "n2".into(),
                        host: "10.0.0.2".into(),
                        port: 7000,
                    },
                ],
                slots: vec![
                    TopoSlotRange {
                        start: 0,
                        end: 8191,
                        owner_id: Some("n1".into()),
                    },
                    TopoSlotRange {
                        start: 8192,
                        end: 16383,
                        owner_id: Some("n2".into()),
                    },
                ],
            },
            raft: Some(TopoRaft {
                is_leader: true,
                leader_id: Some(1),
                term: 3,
                commit_index: 42,
                voters: 3,
            }),
            replication: TopoReplication {
                role: "master".into(),
            },
        });
        let resp = handle("/api/cluster", Some(&t), &ctx());
        let v = parse(&resp);
        let ct = &v["cluster_topology"];
        assert_eq!(ct["mode"], "raft");
        assert_eq!(ct["enabled"], true);
        assert_eq!(ct["committed_epoch"], 9);
        assert_eq!(ct["members"], 2);
        // The two ranges cover the whole 16384-slot space, owned by 2 distinct nodes.
        assert_eq!(ct["slots_assigned"], 16384);
        assert_eq!(ct["slot_owners"], 2);
        assert_eq!(ct["raft"]["is_leader"], true);
        assert_eq!(ct["raft"]["term"], 3);
        assert_eq!(ct["raft"]["voters"], 3);
    }

    #[test]
    fn nodes_returns_summaries_with_hit_ratio() {
        let t = topo();
        let resp = handle("/api/nodes", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert!(v.is_array());
        let n0 = &v[0];
        assert_eq!(n0["addr"], "10.0.0.1:6379");
        assert_eq!(n0["reachable"], true);
        assert_eq!(n0["version"], "7.2.4");
        assert_eq!(n0["keys"], 15);
        assert_eq!(n0["connected_clients"], 3);
        // 80 / (80 + 20) = 0.8
        assert!((n0["hit_ratio"].as_f64().unwrap() - 0.8).abs() < 1e-9);
        assert!(n0["error"].is_null());
    }

    #[test]
    fn node_detail_found_and_not_found() {
        let t = topo();
        // Found (plain addr).
        let resp = handle("/api/nodes/10.0.0.1:6379", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["addr"], "10.0.0.1:6379");
        assert_eq!(v["slowlog"][0]["argv"][0], "GET");
        assert_eq!(v["clients"][0]["id"], 7);
        // Found (percent-encoded colon).
        let resp = handle("/api/nodes/10.0.0.1%3A6379", Some(&t), &ctx());
        assert_eq!(resp.status, 200, "percent-encoded addr must resolve");
        // Not found.
        let resp = handle("/api/nodes/9.9.9.9:1", Some(&t), &ctx());
        assert_eq!(resp.status, 404);
        let v = parse(&resp);
        assert!(v["error"].is_string());
    }

    #[test]
    fn slowlog_and_clients_are_shaped_per_node() {
        let t = topo();
        let resp = handle("/api/slowlog", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["nodes"][0]["addr"], "10.0.0.1:6379");
        assert_eq!(v["nodes"][0]["entries"][0]["micros"], 12_000);
        assert!(v["nodes"][0]["error"].is_null());

        let resp = handle("/api/clients", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["nodes"][0]["clients"][0]["id"], 7);
    }

    #[test]
    fn keyspace_breaks_down_per_db_and_totals() {
        let t = topo();
        let resp = handle("/api/keyspace", Some(&t), &ctx());
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["total_keys"], 15);
        let per_db = v["per_db"].as_array().unwrap();
        assert_eq!(per_db.len(), 2);
        // Sorted by db name: db0 then db1.
        assert_eq!(per_db[0]["db"], "db0");
        assert_eq!(per_db[0]["keys"], 10);
        assert_eq!(per_db[0]["expires"], 2);
        assert_eq!(per_db[1]["db"], "db1");
        assert_eq!(per_db[1]["keys"], 5);
    }

    #[test]
    fn unknown_api_path_is_404() {
        let t = topo();
        let resp = handle("/api/bogus", Some(&t), &ctx());
        assert_eq!(resp.status, 404);
        assert!(parse(&resp)["error"].is_string());
    }

    #[test]
    fn openapi_document_is_valid_json_with_required_keys() {
        let resp = handle("/api/openapi.json", None, &ctx());
        assert_eq!(resp.status, 200);
        let v: serde_json::Value =
            serde_json::from_str(&resp.body).expect("openapi.json must be valid JSON");
        assert_eq!(v["openapi"], "3.0.3");
        assert!(v["info"]["title"].is_string());
        assert!(v["info"]["version"].is_string());
        let paths = v["paths"].as_object().expect("paths must be an object");
        for p in [
            "/api/health",
            "/api/cluster",
            "/api/nodes",
            "/api/nodes/{addr}",
            "/api/slowlog",
            "/api/clients",
            "/api/keyspace",
            "/api/timeseries",
            "/api/openapi.json",
            // The node-level management surface (#361).
            "/api/config",
            "/api/keys",
            "/api/keys/{k}",
            "/api/keys/{k}/expire",
            "/api/keys/{k}/persist",
            "/api/command",
            "/api/pubsub/channels",
            "/api/pubsub/publish",
            "/api/acl",
            "/api/acl/user",
            "/api/acl/user/{name}",
            "/api/persistence",
            "/api/persistence/save",
            "/api/cluster/rebalance-plan",
            "/api/cluster/failover",
            "/api/cluster/meet",
            "/api/cluster/forget",
            "/api/cluster/setslot",
        ] {
            assert!(paths.contains_key(p), "openapi missing path {p}");
        }
        // The management write paths document their mutating verb.
        assert!(paths["/api/config"].get("post").is_some(), "config POST");
        assert!(
            paths["/api/cluster/failover"].get("post").is_some(),
            "failover POST"
        );
        assert!(
            paths["/api/cluster/meet"].get("post").is_some(),
            "meet POST"
        );
        assert!(
            paths["/api/cluster/forget"].get("post").is_some(),
            "forget POST"
        );
        assert!(paths["/api/keys/{k}"].get("delete").is_some(), "key DELETE");
        assert!(paths["/api/command"].get("post").is_some(), "command POST");
        assert!(
            paths["/api/acl/user/{name}"].get("delete").is_some(),
            "acl deluser DELETE"
        );
    }

    #[test]
    fn is_api_path_matches_the_namespace() {
        assert!(is_api_path("/api"));
        assert!(is_api_path("/api/health"));
        assert!(is_api_path("/api/nodes/10.0.0.1:6379"));
        assert!(!is_api_path("/metrics"));
        assert!(!is_api_path("/apiconfusion"));
    }

    #[test]
    fn percent_decode_handles_colon_and_malformed() {
        assert_eq!(percent_decode("10.0.0.1%3A6379"), "10.0.0.1:6379");
        assert_eq!(percent_decode("10.0.0.1:6379"), "10.0.0.1:6379");
        // A malformed escape is left verbatim, not an error.
        assert_eq!(percent_decode("a%zz"), "a%zz");
        assert_eq!(percent_decode("trailing%"), "trailing%");
    }

    #[test]
    fn error_body_escapes_special_characters() {
        let body = error_body("bad \"quote\" and \n newline");
        let v: serde_json::Value = serde_json::from_str(&body).expect("error body must be JSON");
        assert_eq!(v["error"], "bad \"quote\" and \n newline");
    }

    #[test]
    fn node_snapshot_serde_round_trips_through_value() {
        // Serialize a snapshot to JSON and back through serde_json::Value, proving
        // the response types serialize to a stable, parseable shape.
        let node = sample_node();
        let json = serde_json::to_string(&node).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["addr"], "10.0.0.1:6379");
        assert_eq!(v["reachable"], true);
        assert_eq!(v["info"]["redis_version"], "7.2.4");
        assert_eq!(v["slowlog"][0]["argv"][1], "foo");
        assert_eq!(v["clients"][0]["addr"], "127.0.0.1:6379");
        assert!(v["slowlog_error"].is_null());
    }

    // ---- /api/timeseries (#356) -------------------------------------------

    /// A stub [`HistorySource`] that records the args it was called with and
    /// returns a canned series, so the endpoint can be tested without a network.
    struct StubSource {
        calls: std::sync::Mutex<Vec<(String, u64, u64, u64)>>,
        result: Result<Vec<TimeSeries>, String>,
    }

    impl StubSource {
        fn ok(series: Vec<TimeSeries>) -> Self {
            StubSource {
                calls: std::sync::Mutex::new(Vec::new()),
                result: Ok(series),
            }
        }

        fn err(message: &str) -> Self {
            StubSource {
                calls: std::sync::Mutex::new(Vec::new()),
                result: Err(message.to_owned()),
            }
        }
    }

    impl HistorySource for StubSource {
        fn query_range<'a>(
            &'a self,
            metric: &'a str,
            start_unix: u64,
            end_unix: u64,
            step_secs: u64,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<TimeSeries>, HistoryError>> + Send + 'a,
            >,
        > {
            // Defensive re-check mirrors the real adapter (the API edge should have
            // already rejected a disallowed metric before we are called).
            let metric = metric.to_owned();
            Box::pin(async move {
                if let Ok(mut calls) = self.calls.lock() {
                    calls.push((metric.clone(), start_unix, end_unix, step_secs));
                }
                match &self.result {
                    Ok(series) => Ok(series.clone()),
                    Err(msg) => Err(HistoryError::Source(msg.clone())),
                }
            })
        }
    }

    fn sample_series() -> Vec<TimeSeries> {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert(
            "__name__".to_owned(),
            "ironcache_used_memory_bytes".to_owned(),
        );
        vec![TimeSeries {
            labels,
            points: vec![(1000, 1.0), (1015, 2.0)],
        }]
    }

    #[tokio::test]
    async fn timeseries_503_when_no_source_configured() {
        let resp = handle_async("/api/timeseries?metric=ironcache_x", None, None, &ctx()).await;
        assert_eq!(resp.status, 503);
        let v = parse(&resp);
        assert!(v["error"].as_str().unwrap().contains("no history source"));
    }

    #[tokio::test]
    async fn timeseries_503_via_sync_handle_when_unwired() {
        // The sync handle (used by unit tests / when no source is threaded) also
        // reports the unconfigured case for the history route.
        let resp = handle("/api/timeseries", None, &ctx());
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn timeseries_200_shape_with_a_wired_source() {
        let src = StubSource::ok(sample_series());
        let resp = handle_async(
            "/api/timeseries?metric=ironcache_used_memory_bytes&range=600&step=30",
            None,
            Some(&src),
            &ctx(),
        )
        .await;
        assert_eq!(resp.status, 200);
        let v = parse(&resp);
        assert_eq!(v["metric"], "ironcache_used_memory_bytes");
        assert_eq!(v["step_secs"], 30);
        // now_unix (1_700_000_000) minus range 600.
        assert_eq!(v["start_unix"], 1_699_999_400u64);
        assert_eq!(v["end_unix"], 1_700_000_000u64);
        assert_eq!(v["series"][0]["points"][0][0], 1000);
        assert_eq!(v["series"][0]["points"][0][1], 1.0);
        // The source was called with the resolved window.
        let calls = src.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            (
                "ironcache_used_memory_bytes".to_owned(),
                1_699_999_400,
                1_700_000_000,
                30
            )
        );
    }

    #[tokio::test]
    async fn timeseries_400_on_missing_metric() {
        let src = StubSource::ok(Vec::new());
        let resp = handle_async("/api/timeseries", None, Some(&src), &ctx()).await;
        assert_eq!(resp.status, 400);
        assert!(parse(&resp)["error"].as_str().unwrap().contains("metric"));
        // The disallowed/missing metric never reached the source.
        assert!(src.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeseries_400_on_disallowed_metric_injection() {
        let src = StubSource::ok(Vec::new());
        // A PromQL injection attempt via the metric param.
        let resp = handle_async(
            "/api/timeseries?metric=rate(up%5B5m%5D)",
            None,
            Some(&src),
            &ctx(),
        )
        .await;
        assert_eq!(resp.status, 400);
        let v = parse(&resp);
        assert!(v["error"].as_str().unwrap().contains("not an allowed"));
        // SSRF/injection guard: the source was NOT called.
        assert!(src.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeseries_400_on_non_ironcache_metric() {
        let src = StubSource::ok(Vec::new());
        let resp = handle_async(
            "/api/timeseries?metric=node_cpu_seconds_total",
            None,
            Some(&src),
            &ctx(),
        )
        .await;
        assert_eq!(resp.status, 400);
        assert!(src.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeseries_400_on_bad_numeric_param() {
        let src = StubSource::ok(Vec::new());
        let resp = handle_async(
            "/api/timeseries?metric=ironcache_x&range=abc",
            None,
            Some(&src),
            &ctx(),
        )
        .await;
        assert_eq!(resp.status, 400);
        assert!(parse(&resp)["error"].as_str().unwrap().contains("range"));
    }

    #[tokio::test]
    async fn timeseries_502_on_source_error() {
        let src = StubSource::err("Prometheus is down");
        let resp = handle_async(
            "/api/timeseries?metric=ironcache_x",
            None,
            Some(&src),
            &ctx(),
        )
        .await;
        assert_eq!(resp.status, 502);
        assert!(
            parse(&resp)["error"]
                .as_str()
                .unwrap()
                .contains("Prometheus is down")
        );
    }

    #[test]
    fn resolve_window_defaults_and_clamps() {
        // Default range/step over a fixed now.
        let (s, e, step) = resolve_window(10_000, DEFAULT_RANGE_SECS, DEFAULT_STEP_SECS);
        assert_eq!(e, 10_000);
        assert_eq!(s, 10_000 - DEFAULT_RANGE_SECS);
        assert_eq!(step, DEFAULT_STEP_SECS);
        // A range over the cap is clamped.
        let (s, _e, _step) = resolve_window(MAX_RANGE_SECS * 2, MAX_RANGE_SECS * 2, 60);
        assert_eq!(s, MAX_RANGE_SECS); // now - MAX_RANGE
        // A tiny step over a wide range raises the step so points <= the cap.
        let (_s, _e, step) = resolve_window(MAX_RANGE_SECS, MAX_RANGE_SECS, 1);
        assert!(
            MAX_RANGE_SECS / step <= MAX_POINTS_PER_SERIES,
            "step={step}"
        );
        // step floor of MIN_STEP_SECS (a zero step is raised).
        let (_s, _e, step) = resolve_window(100, 50, 0);
        assert!(step >= MIN_STEP_SECS);
    }

    #[test]
    fn query_params_parse_and_decode() {
        let q = QueryParams::parse("metric=ironcache_x&range=600&step=30");
        assert_eq!(q.get("metric").as_deref(), Some("ironcache_x"));
        assert_eq!(q.get_u64("range").unwrap(), Some(600));
        assert_eq!(q.get_u64("step").unwrap(), Some(30));
        assert_eq!(q.get("absent"), None);
        assert_eq!(q.get_u64("absent").unwrap(), None);
        // A bad integer is an Err (mapped to 400 by the caller).
        let q = QueryParams::parse("range=notanumber");
        assert!(q.get_u64("range").is_err());
        // Percent + plus decoding.
        let q = QueryParams::parse("metric=ironcache_x&note=a%20b+c");
        assert_eq!(q.get("note").as_deref(), Some("a b c"));
    }
}
