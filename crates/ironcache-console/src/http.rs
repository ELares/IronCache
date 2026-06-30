// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console's bounded, hand-rolled tokio HTTP/1.1 responder (issue #353).
//!
//! It serves the fixed probe/metrics routes:
//!   * `GET /metrics` -> the console's OWN Prometheus self-metrics,
//!   * `GET /livez`   -> `200` once the process is up (a liveness probe), and
//!   * `GET /readyz`  -> `200` when the console is ready to serve (a readiness probe),
//!
//! plus the JSON REST API at `/api/*` (#358, handled in [`crate::api`]). The SPA
//! (#359) hangs off this same server later. It is hand-rolled (no hyper/axum) for
//! the same reason the engine's metrics endpoint is: a tiny route surface keeps
//! the static musl build pure-Rust and adds no new HTTP-server dependency. It
//! bounds each request (a whole-request deadline, a small header cap, a
//! connection-concurrency cap) and is NOT a general HTTP server: anything
//! malformed/oversized gets a fixed error + close. The `/api/*` routes go through
//! that SAME bounded responder, so the deadline/size-cap/permit still apply.
//!
//! SECURITY: the `/api/*` surface exposes node internals (node addresses, slowlog
//! argv = key names, client IPs). It is UNAUTHENTICATED today and relies on the
//! loopback default bind; it MUST move behind the auth/RBAC tier (#360) and the
//! VPN-locked exposure (#369) before the console is exposed. See [`crate::api`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::api::{self, ApiContext, ApiResponse};
use crate::auth::{self, AuthPolicy, Decision, Tier};
use crate::history::HistorySource;
use crate::metrics::ConsoleMetrics;
use crate::node::NodeAccess;
use crate::poll::{TopologyHolder, new_topology_holder};

/// Max request bytes before a `413` (probes send only a request line + a few
/// headers, never a body); bounds the per-connection buffer.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// The WHOLE-REQUEST deadline: the entire request-read phase must complete in
/// this window, so a slow-drip (slowloris) client cannot hold the socket.
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// Max connections served concurrently; the accept loop drops the excess rather
/// than queueing unbounded tasks.
const MAX_CONCURRENT_CONNS: usize = 128;

/// The shared state the HTTP handler reads at request time. Cheap, lock-free
/// reads; cloned (`Arc` inside) into each connection task.
#[derive(Clone)]
pub struct ConsoleHttpState {
    metrics: Arc<ConsoleMetrics>,
    /// Liveness: set `true` at the end of boot; never flips back.
    live: Arc<AtomicBool>,
    /// Readiness: set `true` on the FIRST successful node poll (#355). The poll
    /// loop owns this flip, so `/readyz` is 503 until the console has real data.
    ready: Arc<AtomicBool>,
    /// The latest polled topology, shared with the poll loop (#355/#366). The
    /// REST API (#358) reads it to render the `/api/*` responses.
    topology: TopologyHolder,
    /// The resolved authentication/RBAC policy (#360). The `/api/*` gate consults
    /// it before dispatching to [`crate::api::handle`]. Cheap to clone (`Arc`).
    auth: Arc<AuthPolicy>,
    /// The configured history source (#356), shared into each request. `None` when
    /// no `prometheus_url` is configured, in which case `/api/timeseries` is 503.
    /// SECURITY: this carries the SERVER-configured Prometheus base URL; the
    /// request never supplies it (the SSRF boundary).
    history: Option<Arc<dyn HistorySource>>,
    /// The on-demand node connection factory (#361), used by the management layer
    /// to open a SHORT-LIVED `NodeClient` per management request (run the command,
    /// then drop). `None` when no seed is configured, in which case the management
    /// endpoints answer `503`. Carries the zeroized node password (never logged).
    node_access: Option<Arc<NodeAccess>>,
}

impl ConsoleHttpState {
    /// Construct with the DEV-DEFAULT auth policy (no token, loopback-trusted), so
    /// existing callers and unit tests keep the historical "serve everything"
    /// behavior. Production boot wires the real policy with
    /// [`Self::with_topology_and_auth`].
    #[must_use]
    pub fn new(metrics: Arc<ConsoleMetrics>) -> Self {
        Self::with_topology(metrics, new_topology_holder())
    }

    /// Construct with an EXISTING topology holder, the DEV-DEFAULT auth policy (no
    /// token, loopback-trusted), and no history source, so the poll loop and the
    /// HTTP surface share one cell (the loop writes, the handler reads through the
    /// REST API). Production boot overrides auth + history via
    /// [`Self::with_topology_and_auth`] + [`Self::with_history`].
    #[must_use]
    pub fn with_topology(metrics: Arc<ConsoleMetrics>, topology: TopologyHolder) -> Self {
        Self::with_topology_and_auth(metrics, topology, AuthPolicy::resolve(None, None, true))
    }

    /// Construct with an existing topology holder AND an explicit auth policy
    /// (#360). Production boot uses this with the policy resolved from the config
    /// tokens and the bind classification.
    #[must_use]
    pub fn with_topology_and_auth(
        metrics: Arc<ConsoleMetrics>,
        topology: TopologyHolder,
        auth: AuthPolicy,
    ) -> Self {
        ConsoleHttpState {
            metrics,
            live: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false)),
            topology,
            auth: Arc::new(auth),
            history: None,
            node_access: None,
        }
    }

    /// Attach a history source (the Prometheus adapter, #356), consuming and
    /// returning `self` (builder style) so `lib.rs` can wire it after constructing
    /// the state with the shared topology holder.
    #[must_use]
    pub fn with_history(mut self, history: Option<Arc<dyn HistorySource>>) -> Self {
        self.history = history;
        self
    }

    /// Attach the on-demand node connection factory (#361), consuming and
    /// returning `self` (builder style). Production boot wires this with the
    /// resolved seed + auth + tls + timeouts so the management endpoints can open a
    /// short-lived `NodeClient` per request. With `None` (no seed) the management
    /// endpoints answer `503`.
    #[must_use]
    pub fn with_node_access(mut self, node_access: Option<Arc<NodeAccess>>) -> Self {
        self.node_access = node_access;
        self
    }

    /// The shared topology holder (so `lib.rs` can hand the same cell to the poll
    /// loop it handed the HTTP state).
    #[must_use]
    pub fn topology(&self) -> TopologyHolder {
        self.topology.clone()
    }

    /// Flip liveness (called once at end of boot).
    pub fn set_live(&self, v: bool) {
        self.live.store(v, Ordering::SeqCst);
    }

    /// Flip readiness.
    pub fn set_ready(&self, v: bool) {
        self.ready.store(v, Ordering::SeqCst);
    }

    /// Render the response bytes for a parsed `(method, path)`. Reads the live /
    /// ready state and the latest topology and returns the bytes; the connection
    /// handler writes them. Async because the `/api/*` routes read the shared
    /// topology behind an async `RwLock`. Exposed for tests.
    ///
    /// The `/api/*` namespace (#358) is dispatched to [`crate::api`] here; all
    /// other paths fall through to the fixed-route [`Self::respond`]. The API goes
    /// through this SAME bounded responder, so the whole-request deadline, the
    /// size cap, and the concurrency permit still apply.
    pub async fn respond_async(
        &self,
        method: &str,
        path: &str,
        auth_header: Option<&str>,
    ) -> Vec<u8> {
        // The read path (GET/HEAD) carries no body; delegate to the body-aware form
        // with an empty body so the two share one implementation.
        self.respond_async_with_body(method, path, auth_header, &[])
            .await
    }

    /// [`Self::respond_async`] WITH the (already bounded) request body, so the
    /// management write endpoints (#361) can read their JSON. The body is the bytes
    /// the connection handler read after the header block, capped by
    /// [`MAX_REQUEST_BYTES`]; an oversized body never reaches here (the read phase
    /// answers `413` first). GET/HEAD ignore the body.
    pub async fn respond_async_with_body(
        &self,
        method: &str,
        path: &str,
        auth_header: Option<&str>,
        body: &[u8],
    ) -> Vec<u8> {
        let head = method == "HEAD";
        let bare = path.split('?').next().unwrap_or(path);
        if !api::is_api_path(bare) {
            // The UI/probe/static routes are GET/HEAD-only; a non-GET there stays a
            // 405 via the fixed responder (which carries no API headers).
            return self.respond(method, path);
        }

        let is_read = method == "GET" || head;
        let is_write = matches!(method, "POST" | "DELETE" | "PUT" | "PATCH");
        if !is_read && !is_write {
            // An unknown method on an /api/* route: 405 with the API headers.
            return api_response(405, status_reason(405), b"", head);
        }

        // AUTH/RBAC GATE (#360, #361): every `/api/*` route maps to a tier from the
        // METHOD + path; the policy decides allow / 401 / 403 from the (bind, token)
        // posture and the presented Bearer token. This runs BEFORE any handler, so
        // privileged data is never produced and NO mutation runs for a request that
        // is not authorized. A write verb maps to `Admin` regardless of path (fail
        // closed), so a trailing slash / casing / method trick cannot drop a
        // mutation below the admin bar.
        let required = auth::route_tier_for_method(method, bare);
        match auth::authorize(&self.auth, required, auth_header) {
            Decision::Allow => {}
            Decision::Unauthorized(reason) => {
                return api_response(
                    401,
                    status_reason(401),
                    api::error_json(reason).as_bytes(),
                    head,
                );
            }
            Decision::Forbidden(reason) => {
                return api_response(
                    403,
                    status_reason(403),
                    api::error_json(reason).as_bytes(),
                    head,
                );
            }
        }

        // Write verbs go to the management dispatch (#361). The tier gate above has
        // already enforced Admin, so by here the caller is authorized to mutate.
        // A write to a path that is NOT a known management WRITE endpoint (e.g. a
        // POST to the read-only `/api/cluster`) is a 405: the route exists but does
        // not accept this method. This is decided BEFORE any node connection, so an
        // unknown write never leaks node-reachability state.
        if is_write {
            if !is_management_write(method, bare) {
                return api_response(405, status_reason(405), b"", head);
            }
            let resp = self.handle_manage(method, path, bare, body, required).await;
            return api_response(
                resp.status,
                status_reason(resp.status),
                resp.body.as_bytes(),
                head,
            );
        }

        // A read of a MANAGEMENT route also goes through the node (CONFIG GET, the
        // SCAN browser, the key inspector, the channel list, persistence, ACL list).
        if is_management_read(bare) {
            let resp = self.handle_manage(method, path, bare, body, required).await;
            return api_response(
                resp.status,
                status_reason(resp.status),
                resp.body.as_bytes(),
                head,
            );
        }

        let ctx = ApiContext {
            version: crate::cli::BUILD_VERSION,
            live: self.live.load(Ordering::SeqCst),
            ready: self.ready.load(Ordering::SeqCst),
            uptime_seconds: self.metrics.uptime_seconds(),
            // "now" via the same env clock seam the metrics use (#356), never
            // SystemTime::now directly.
            now_unix: self.metrics.now_unix_seconds(),
        };
        // The history route does I/O (a Prometheus query) and does NOT need the
        // topology, so handle it WITHOUT holding the topology read lock: holding
        // it across a slow upstream query would block the poll loop's write for
        // up to the request deadline. Every OTHER route is pure over the
        // topology, so the guard is held only for those and is dropped promptly.
        let resp = if bare == "/api/timeseries" {
            let query = path.split_once('?').map_or("", |(_, q)| q);
            api::handle_timeseries(query, self.history.as_deref(), &ctx).await
        } else {
            let guard = self.topology.read().await;
            api::handle(bare, guard.as_ref(), &ctx)
        };
        api_response(
            resp.status,
            status_reason(resp.status),
            resp.body.as_bytes(),
            head,
        )
    }

    /// Dispatch a MANAGEMENT request (#361) to the node. Opens a short-lived
    /// `NodeClient` via the on-demand factory (so the topology read lock is NEVER
    /// held across node I/O), runs the handler, audit-logs a mutation, and returns
    /// the JSON `ApiResponse`. The tier gate in [`Self::respond_async_with_body`]
    /// has ALREADY enforced the required tier before this runs.
    ///
    /// `tier` is the authorized tier (for the audit line); `raw_path` keeps the
    /// query string (the SCAN browser reads `?pattern=&cursor=&count=`), `bare` is
    /// the query-stripped path used for dispatch, and `body` is the bounded request
    /// body. With no node access configured (no seed) every route here answers
    /// `503`. The audit line logs only the verb + the QUERY-STRIPPED path (never a
    /// query value or a body), so a key name in the path is recorded but no value /
    /// secret ever is.
    async fn handle_manage(
        &self,
        method: &str,
        raw_path: &str,
        bare: &str,
        body: &[u8],
        tier: Tier,
    ) -> ApiResponse {
        let Some(access) = self.node_access.as_ref() else {
            return ApiResponse::error(503, "no seed node configured; management is unavailable");
        };
        let mut client = match access.connect().await {
            Ok(c) => c,
            Err(e) => {
                return ApiResponse::error(502, &format!("could not connect to the node: {e}"));
            }
        };
        let is_mutation = matches!(method, "POST" | "DELETE" | "PUT" | "PATCH");
        let resp = dispatch_manage(&mut client, method, raw_path, bare, body).await;
        // AUDIT (#361): log every MUTATION (action + target + result + tier).
        // NEVER log a body/value/secret: the action verb + the query-stripped path
        // + status only.
        if is_mutation {
            tracing::info!(
                action = %method,
                target = %bare,
                status = resp.status,
                tier = ?tier,
                "console management mutation"
            );
        }
        resp
    }

    /// Render the response bytes for the FIXED routes (`/metrics`, `/livez`,
    /// `/readyz`, and the 404/405 fallbacks). Pure: reads only the atomic flags.
    /// Exposed for tests; `/debug/topology` goes through [`Self::respond_async`].
    #[must_use]
    pub fn respond(&self, method: &str, path: &str) -> Vec<u8> {
        let head = method == "HEAD";
        if method != "GET" && !head {
            return http_response(
                405,
                "Method Not Allowed",
                "text/plain; charset=utf-8",
                b"",
                head,
            );
        }
        let path = path.split('?').next().unwrap_or(path);
        match path {
            // The dashboard SPA (#359): static assets embedded with `include_str!`
            // and served off this same responder. They need no topology, so they
            // live here in the sync `respond`. Each carries the strict UI security
            // headers (CSP, nosniff, frame-deny, no-referrer): CSS and JS are
            // SEPARATE files so the CSP `default-src 'self'` needs no
            // 'unsafe-inline'.
            //
            // SECURITY: the dashboard reads the unauthenticated `/api/*` recon
            // surface (node addresses, slowlog argv = key names, client IPs). The
            // UI is UNAUTHENTICATED today and relies on the loopback default bind;
            // it MUST move behind the auth/RBAC tier (#360) and VPN-locked
            // exposure (#369) before the console is exposed. See [`crate::api`].
            "/" => ui_response(
                "text/html; charset=utf-8",
                crate::assets::INDEX_HTML.as_bytes(),
                head,
            ),
            "/app.css" => ui_response(
                "text/css; charset=utf-8",
                crate::assets::APP_CSS.as_bytes(),
                head,
            ),
            "/app.js" => ui_response(
                "application/javascript; charset=utf-8",
                crate::assets::APP_JS.as_bytes(),
                head,
            ),
            // The self-hosted fonts (no CDN, for the strict CSP). The stylesheet
            // is `@import`ed by `app.css` and references the two woff2 by relative
            // URL; each carries the same strict UI security headers. The woff2 are
            // BINARY: served as raw bytes with `Content-Type: font/woff2`.
            "/assets/fonts.css" => ui_response(
                "text/css; charset=utf-8",
                crate::assets::FONTS_CSS.as_bytes(),
                head,
            ),
            "/assets/fonts/hanken-grotesk.woff2" => {
                ui_response("font/woff2", crate::assets::FONT_HANKEN_GROTESK_WOFF2, head)
            }
            "/assets/fonts/jetbrains-mono.woff2" => {
                ui_response("font/woff2", crate::assets::FONT_JETBRAINS_MONO_WOFF2, head)
            }
            "/metrics" => http_response(
                200,
                "OK",
                "text/plain; version=0.0.4; charset=utf-8",
                self.metrics.render().as_bytes(),
                head,
            ),
            "/livez" => {
                if self.live.load(Ordering::SeqCst) {
                    http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n", head)
                } else {
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        b"starting\n",
                        head,
                    )
                }
            }
            "/readyz" => {
                if self.ready.load(Ordering::SeqCst) {
                    http_response(200, "OK", "text/plain; charset=utf-8", b"OK\n", head)
                } else {
                    http_response(
                        503,
                        "Service Unavailable",
                        "text/plain; charset=utf-8",
                        b"not ready\n",
                        head,
                    )
                }
            }
            _ => http_response(
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
                head,
            ),
        }
    }
}

/// Whether `path` (query-stripped) is a MANAGEMENT READ that must reach the node
/// (#361): `CONFIG GET`, the SCAN key browser, the key inspector, the channel
/// list, persistence status, and the ACL list. These are NOT served from the
/// polled topology; they run a live command. (The READ tier is decided separately
/// by [`auth::route_tier_for_method`]; this only routes a GET to the node path.)
fn is_management_read(path: &str) -> bool {
    matches!(
        path,
        "/api/config"
            | "/api/keys"
            | "/api/pubsub/channels"
            | "/api/acl"
            | "/api/persistence"
            | "/api/cluster/rebalance-plan"
    ) || path.starts_with("/api/keys/")
}

/// Whether `(method, path)` is a KNOWN management WRITE endpoint (#361). A write
/// to a path NOT in this set (e.g. a `POST` to the read-only `/api/cluster`, or a
/// wholly unknown path) is answered `405` BEFORE any node connection, so the write
/// surface is explicit and an unknown write never reaches the node. The dynamic
/// `/api/keys/{k}` family (`POST`/`DELETE`, plus the `expire`/`persist` POSTs) and
/// the `/api/acl/user/{name}` DELETE are matched by prefix.
fn is_management_write(method: &str, path: &str) -> bool {
    match (method, path) {
        (
            "POST",
            "/api/config"
            | "/api/command"
            | "/api/pubsub/publish"
            | "/api/acl/user"
            | "/api/persistence/save"
            | "/api/cluster/failover"
            | "/api/cluster/meet"
            | "/api/cluster/forget"
            | "/api/cluster/setslot",
        ) => true,
        // The dynamic key family: a bare `/api/keys` POST is NOT a write (SET needs
        // a key), but `/api/keys/{k}` POST (SET) and DELETE (DEL) are.
        ("POST" | "DELETE", p) if p.starts_with("/api/keys/") => true,
        ("DELETE", p) if p.starts_with("/api/acl/user/") => true,
        _ => false,
    }
}

/// Parse a management request body as `T` with `serde_json`, mapping any failure
/// (malformed / wrong-shape / empty JSON) to a `400` JSON [`ApiResponse`], NEVER a
/// panic. The body was already bounded by [`MAX_REQUEST_BYTES`] in the read phase.
fn parse_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ApiResponse> {
    serde_json::from_slice::<T>(body)
        .map_err(|e| ApiResponse::error(400, &format!("invalid JSON request body: {e}")))
}

/// Route one MANAGEMENT request (already authorized + connected) to its handler.
/// The `(method, bare)` match keeps the dispatch flat and explicit; the dynamic
/// `/api/keys/{k}` family is matched by prefix with the key URL-decoded. `raw_path`
/// keeps the query string for the SCAN browser. A body parse failure
/// short-circuits to a `400` (via [`parse_body`]).
async fn dispatch_manage(
    client: &mut crate::node::NodeClient,
    method: &str,
    raw_path: &str,
    bare: &str,
    body: &[u8],
) -> ApiResponse {
    use crate::manage;
    match (method, bare) {
        // ---- cluster management (#361) ----
        // Read-only rebalance dry-run plan (the slot diff before any apply). Admin-tier
        // via ADMIN_READ_ROUTES; the engine refuses APPLY, so this never mutates.
        ("GET", "/api/cluster/rebalance-plan") => manage::cluster_rebalance_plan(client).await,
        // MUTATING: trigger a bare CLUSTER FAILOVER (engine-gated to in-sync replicas).
        // Admin-tier (a write) + a typed destructive-confirmation in the body.
        ("POST", "/api/cluster/failover") => match parse_body::<manage::FailoverBody>(body) {
            Ok(b) => manage::cluster_failover(client, &b).await,
            Err(resp) => resp,
        },
        // MUTATING: add a node (CLUSTER MEET, additive) / remove a node (CLUSTER FORGET,
        // destructive: confirm must echo the node id). Both Admin-tier writes.
        ("POST", "/api/cluster/meet") => match parse_body::<manage::MeetBody>(body) {
            Ok(b) => manage::cluster_meet(client, &b).await,
            Err(resp) => resp,
        },
        ("POST", "/api/cluster/forget") => match parse_body::<manage::ForgetBody>(body) {
            Ok(b) => manage::cluster_forget(client, &b).await,
            Err(resp) => resp,
        },
        // MUTATING: the online-migration / slot-FLIP control (CLUSTER SETSLOT). Destructive:
        // confirm must echo the slot. Admin-tier write.
        ("POST", "/api/cluster/setslot") => match parse_body::<manage::SetslotBody>(body) {
            Ok(b) => manage::cluster_setslot(client, &b).await,
            Err(resp) => resp,
        },
        // ---- config ----
        ("GET", "/api/config") => manage::config_get(client).await,
        ("POST", "/api/config") => match parse_body::<manage::ConfigSetBody>(body) {
            Ok(b) => manage::config_set(client, &b).await,
            Err(resp) => resp,
        },
        // ---- command console ----
        ("POST", "/api/command") => match parse_body::<manage::CommandBody>(body) {
            Ok(b) => manage::run_command(client, &b).await,
            Err(resp) => resp,
        },
        // ---- pub/sub ----
        ("GET", "/api/pubsub/channels") => manage::pubsub_channels(client).await,
        ("POST", "/api/pubsub/publish") => match parse_body::<manage::PublishBody>(body) {
            Ok(b) => manage::pubsub_publish(client, &b).await,
            Err(resp) => resp,
        },
        // ---- acl ----
        ("GET", "/api/acl") => manage::acl_get(client).await,
        ("POST", "/api/acl/user") => match parse_body::<manage::AclUserBody>(body) {
            Ok(b) => manage::acl_setuser(client, &b).await,
            Err(resp) => resp,
        },
        ("DELETE", p) if p.starts_with("/api/acl/user/") => {
            // `strip_prefix` removes the prefix exactly once (a name that itself
            // begins with the prefix text is not over-stripped).
            let name = p
                .strip_prefix("/api/acl/user/")
                .map_or_else(String::new, api::percent_decode_path);
            manage::acl_deluser(client, &name).await
        }
        // ---- persistence ----
        ("GET", "/api/persistence") => manage::persistence_get(client).await,
        ("POST", "/api/persistence/save") => match parse_body::<manage::SaveBody>(body) {
            // An empty body is allowed (defaults to background save).
            Ok(b) => manage::persistence_save(client, &b).await,
            Err(_) if body.is_empty() => {
                manage::persistence_save(client, &manage::SaveBody::default()).await
            }
            Err(resp) => resp,
        },
        // ---- keys (static page + dynamic {k}) ----
        ("GET", "/api/keys") => {
            // The SCAN browser reads `?pattern=&cursor=&count=` from the raw path.
            let q = raw_path.split_once('?').map_or("", |(_, q)| q);
            let params = api::QueryParams::parse(q);
            let cursor = params.get("cursor").unwrap_or_default();
            let pattern = params.get("pattern").unwrap_or_default();
            let count = params
                .get_u64("count")
                .ok()
                .flatten()
                .unwrap_or(manage::DEFAULT_SCAN_COUNT);
            manage::keys_scan(client, &cursor, &pattern, count).await
        }
        (m, p) if p.starts_with("/api/keys/") => dispatch_keys(client, m, p, body).await,
        // Any other (method, path) under management is not a known endpoint.
        _ => ApiResponse::error(
            404,
            &format!("no such management endpoint: {method} {bare}"),
        ),
    }
}

/// Dispatch the dynamic `/api/keys/{k}` family. The `{k}` segment is URL-decoded.
/// Sub-actions: `POST .../expire`, `POST .../persist`; otherwise the bare key:
/// `GET` (inspect), `POST` (string SET), `DELETE`.
async fn dispatch_keys(
    client: &mut crate::node::NodeClient,
    method: &str,
    path: &str,
    body: &[u8],
) -> ApiResponse {
    use crate::manage;
    let Some(suffix) = path.strip_prefix("/api/keys/") else {
        return ApiResponse::error(404, "no such management endpoint");
    };
    // Sub-action suffixes.
    if let Some(name) = suffix.strip_suffix("/expire") {
        if method != "POST" {
            return ApiResponse::error(405, "expire requires POST");
        }
        let key = api::percent_decode_path(name);
        return match parse_body::<manage::ExpireBody>(body) {
            Ok(parsed) => manage::key_expire(client, &key, &parsed).await,
            Err(resp) => resp,
        };
    }
    if let Some(name) = suffix.strip_suffix("/persist") {
        if method != "POST" {
            return ApiResponse::error(405, "persist requires POST");
        }
        let key = api::percent_decode_path(name);
        return manage::key_persist(client, &key).await;
    }
    // The bare key (no sub-action). A trailing slash or an embedded slash is part
    // of the (decoded) key name.
    let key = api::percent_decode_path(suffix);
    match method {
        "GET" => manage::key_get(client, &key).await,
        "POST" => match parse_body::<manage::KeySetBody>(body) {
            Ok(parsed) => manage::key_set(client, &key, &parsed).await,
            Err(resp) => resp,
        },
        "DELETE" => manage::key_delete(client, &key).await,
        _ => ApiResponse::error(405, "method not allowed on a key"),
    }
}

/// The HTTP reason phrase for the status codes the console emits. The default
/// (`200 OK`) covers the success case and any unexpected code defensively.
fn status_reason(code: u16) -> &'static str {
    match code {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

/// The extra response headers carried by the UI assets (the dashboard SPA,
/// #359). A strict CSP that allows ONLY same-origin resources (so the separate
/// `app.css` / `app.js` load while inline script/style and any external/CDN
/// fetch are blocked), plus `X-Content-Type-Options: nosniff`, `X-Frame-Options:
/// DENY`, and `Referrer-Policy: no-referrer`. Each line is `Name: value\r\n`;
/// the builder inserts the block before the blank-line terminator.
const UI_SECURITY_HEADERS: &str = concat!(
    "Content-Security-Policy: default-src 'self'; base-uri 'none'; ",
    "frame-ancestors 'none'; object-src 'none'\r\n",
    "X-Content-Type-Options: nosniff\r\n",
    "X-Frame-Options: DENY\r\n",
    "Referrer-Policy: no-referrer\r\n",
);

/// The extra response headers carried by EVERY `/api/*` JSON response (#369). The
/// API data is sensitive (node addresses, slowlog argv = key names, client IPs)
/// and must NOT be content-sniffed or cached: `X-Content-Type-Options: nosniff`
/// stops a browser from re-typing the `application/json` body, and `Cache-Control:
/// no-store` keeps the privileged data out of any shared/disk cache. These are
/// applied ONLY to `/api/*` (success, 401, 403, 405); the probe routes
/// (`/livez`/`/readyz`/`/metrics`), the UI assets, and their byte-for-byte tests
/// are untouched. Each line is `Name: value\r\n`; the builder inserts the block
/// before the blank-line terminator.
const API_SECURITY_HEADERS: &str = concat!(
    "X-Content-Type-Options: nosniff\r\n",
    "Cache-Control: no-store\r\n",
);

/// Build a complete HTTP/1.1 response (status line, content headers,
/// `Connection: close`, body). One request per connection. When `head` is true
/// the `Content-Length` reflects what a GET would return but NO body bytes are
/// written (RFC 9110: a HEAD response must not carry a message body).
fn http_response(code: u16, reason: &str, content_type: &str, body: &[u8], head: bool) -> Vec<u8> {
    http_response_with_headers(code, reason, content_type, body, head, "")
}

/// A `200 OK` for a static UI asset, carrying the strict [`UI_SECURITY_HEADERS`]
/// in addition to the normal content headers. A HEAD still returns the headers
/// with the correct `Content-Length` but no body.
fn ui_response(content_type: &str, body: &[u8], head: bool) -> Vec<u8> {
    http_response_with_headers(200, "OK", content_type, body, head, UI_SECURITY_HEADERS)
}

/// An `/api/*` JSON response carrying the [`API_SECURITY_HEADERS`] (`nosniff` +
/// `no-store`) on top of the normal content headers (#369). Used for every
/// `/api/*` outcome (success, 401, 403, 405) so the sensitive data is never
/// sniffed or cached; the probe/metrics/UI responses do NOT go through here.
fn api_response(code: u16, reason: &str, body: &[u8], head: bool) -> Vec<u8> {
    http_response_with_headers(
        code,
        reason,
        api::CONTENT_TYPE,
        body,
        head,
        API_SECURITY_HEADERS,
    )
}

/// [`http_response`] with an OPTIONAL block of `extra_headers` (each a complete
/// `Name: value\r\n` line, or empty for none) inserted before the blank-line
/// terminator. The status line, `Content-Type`, `Content-Length`, and
/// `Connection: close` are always emitted (so the existing probe/metrics/api
/// responses are byte-for-byte unchanged when `extra_headers` is empty), and a
/// HEAD still writes no body.
fn http_response_with_headers(
    code: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    head: bool,
    extra_headers: &str,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 256);
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
        body.len()
    );
    out.extend_from_slice(header.as_bytes());
    if !head {
        out.extend_from_slice(body);
    }
    out
}

/// Parse the request LINE (`METHOD SP PATH SP HTTP/x.y`). Returns `Some` once a
/// line terminator is present, `None` if incomplete (read more). A line with too
/// few tokens yields an empty method (answered `405`) or an empty path (answered
/// `404`); it never panics.
fn parse_request_line(buf: &[u8]) -> Option<(String, String)> {
    let line_end = buf.iter().position(|&b| b == b'\n')?;
    let line = &buf[..line_end];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let text = String::from_utf8_lossy(line);
    let mut parts = text.split(' ');
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();
    Some((method, path))
}

/// Whether the buffer holds the END of the request header block (a blank line:
/// `\r\n\r\n` or a bare `\n\n` for tolerance). The responder reads the WHOLE head
/// before answering so the `Authorization` header (after the request line) is
/// available to the auth/RBAC gate (#360). The body, if any, is ignored.
fn header_block_complete(buf: &[u8]) -> bool {
    buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.windows(2).any(|w| w == b"\n\n")
}

/// The byte index just PAST the header-block terminator (`\r\n\r\n` or `\n\n`), so
/// the request body begins at the returned index. `None` if the terminator is not
/// present yet. The `\r\n\r\n` form is preferred (and checked first) so a body that
/// itself contains a bare `\n\n` is not mis-split.
fn header_block_end(buf: &[u8]) -> Option<usize> {
    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
        return Some(pos + 4);
    }
    buf.windows(2).position(|w| w == b"\n\n").map(|pos| pos + 2)
}

/// Parse the `Content-Length` header value from the request head as a `usize`, or
/// `None` when absent / unparseable. Tolerant and panic-free: only the header
/// block is scanned, the field name is case-insensitive, and a non-numeric or
/// negative value yields `None` (treated as no body). The declared length is NOT
/// trusted for allocation: the read loop is bounded by [`MAX_REQUEST_BYTES`]
/// regardless, so a lying large `Content-Length` cannot drive an unbounded read.
fn content_length(buf: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(buf);
    let head = text.split_once("\r\n\r\n").map_or_else(
        || text.split_once("\n\n").map_or(&*text, |(h, _)| h),
        |(h, _)| h,
    );
    head.split('\n')
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
}

/// Extract the FIRST `Authorization` header value from the request head, or
/// `None`. The field name is case-insensitive (RFC 9110); the value is the text
/// after the first colon, trimmed. Parsing is tolerant and never panics: a header
/// line without a colon is skipped, and a non-UTF-8 byte sequence is lossily
/// decoded. Only the header block (up to the first blank line) is scanned.
fn authorization_header(buf: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(buf);
    // Stop at the blank line that ends the header block (ignore any body).
    let head = text.split_once("\r\n\r\n").map_or_else(
        || text.split_once("\n\n").map_or(&*text, |(h, _)| h),
        |(h, _)| h,
    );
    // Skip the request line (the first line); scan the remaining header lines.
    head.split('\n')
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_owned())
}

/// Serve ONE connection with the production whole-request deadline.
async fn serve_conn(stream: tokio::net::TcpStream, state: ConsoleHttpState) {
    serve_conn_with_deadline(stream, state, REQUEST_DEADLINE).await;
}

/// [`serve_conn`] with an explicit deadline so a test can drive the slowloris
/// drop path on a short deadline. The whole read phase is under ONE timeout.
async fn serve_conn_with_deadline(
    mut stream: tokio::net::TcpStream,
    state: ConsoleHttpState,
    deadline: Duration,
) {
    let read_phase = tokio::time::timeout(deadline, async {
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        let mut chunk = [0u8; 1024];
        loop {
            let read = match stream.read(&mut chunk).await {
                Ok(n) if n > 0 => n,
                Ok(_) | Err(_) => return None,
            };
            buf.extend_from_slice(&chunk[..read]);
            if buf.len() > MAX_REQUEST_BYTES {
                return Some(http_response(
                    413,
                    "Payload Too Large",
                    "text/plain; charset=utf-8",
                    b"request too large\n",
                    false,
                ));
            }
            // Read the WHOLE request head (request line + headers) before
            // answering, so the `Authorization` header is available to the
            // auth/RBAC gate (#360). The header block ends at the first blank line.
            // For a method that carries a BODY (#361's POST/DELETE management
            // writes) we then read the declared `Content-Length` bytes too, BOUNDED
            // by the same MAX_REQUEST_BYTES cap (an over-cap body is a 413 above).
            if header_block_complete(&buf) {
                let Some((method, path)) = parse_request_line(&buf) else {
                    // The terminator arrived but no parseable request line: treat as
                    // not found via the fixed responder (never panic).
                    return Some(state.respond("GET", "/"));
                };
                let auth = authorization_header(&buf);
                let header_end = header_block_end(&buf);
                let want_body = content_length(&buf).unwrap_or(0);
                // If a body is declared but not fully read yet, keep reading until
                // it arrives or the size cap / deadline trips. A Content-Length that
                // would push past the cap is caught by the `buf.len()` guard above.
                if let Some(end) = header_end {
                    while buf.len() < end.saturating_add(want_body) {
                        let n = match stream.read(&mut chunk).await {
                            Ok(n) if n > 0 => n,
                            Ok(_) | Err(_) => break, // peer closed: use what we have.
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.len() > MAX_REQUEST_BYTES {
                            return Some(http_response(
                                413,
                                "Payload Too Large",
                                "text/plain; charset=utf-8",
                                b"request too large\n",
                                false,
                            ));
                        }
                    }
                    // The body is the bytes after the header block (clamped to the
                    // declared length so trailing bytes are not interpreted).
                    let body_start = end.min(buf.len());
                    let body_end = end.saturating_add(want_body).min(buf.len());
                    let body = buf[body_start..body_end].to_vec();
                    return Some(
                        state
                            .respond_async_with_body(&method, &path, auth.as_deref(), &body)
                            .await,
                    );
                }
                return Some(state.respond_async(&method, &path, auth.as_deref()).await);
            }
        }
    })
    .await;
    let Ok(Some(response)) = read_phase else {
        return;
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.flush().await;
}

/// The accept loop: accept connections and spawn a bounded [`serve_conn`] per
/// connection. Returns only on an unrecoverable listener error (a transient
/// accept error backs off and continues). At most [`MAX_CONCURRENT_CONNS`] are
/// served at once; the excess is dropped immediately.
pub async fn accept_loop(listener: tokio::net::TcpListener, state: ConsoleHttpState) {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNS));
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    serve_conn(stream, state).await;
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "console http: accept error; backing off");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> ConsoleHttpState {
        ConsoleHttpState::new(Arc::new(ConsoleMetrics::new()))
    }

    /// A state with an EXPLICIT auth policy and a published (empty-node) topology,
    /// so data routes are past the 503-before-poll gate and the auth gate is the
    /// only thing under test.
    fn state_with_auth(auth: AuthPolicy) -> ConsoleHttpState {
        let s = ConsoleHttpState::with_topology_and_auth(
            Arc::new(ConsoleMetrics::new()),
            new_topology_holder(),
            auth,
        );
        s.set_live(true);
        s
    }

    async fn publish_empty_topology(state: &ConsoleHttpState) {
        *state.topology().write().await = Some(crate::snapshot::Topology {
            mode: crate::snapshot::TopologyMode::Standalone,
            nodes: Vec::new(),
            cluster: None,
            fetched_unixtime: 1,
        });
    }

    #[test]
    fn metrics_route_returns_console_prometheus_text() {
        let state = test_state();
        let resp = String::from_utf8(state.respond("GET", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/plain; version=0.0.4"),
            "{resp}"
        );
        assert!(resp.contains("ironcache_console_build_info"), "{resp}");
        assert!(resp.contains("ironcache_console_uptime_seconds"), "{resp}");
    }

    #[test]
    fn livez_flips_with_the_live_flag() {
        let state = test_state();
        let before = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        state.set_live(true);
        let after = String::from_utf8(state.respond("GET", "/livez")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn head_request_has_content_length_but_no_body() {
        let state = test_state();
        let text = String::from_utf8(state.respond("HEAD", "/metrics")).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        let (header, body) = text.split_once("\r\n\r\n").unwrap();
        // Content-Length reflects what a GET would return (non-zero)...
        let cl: usize = header
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            cl > 0,
            "HEAD Content-Length should match the GET body length"
        );
        // ...but no body bytes follow (RFC 9110).
        assert!(body.is_empty(), "HEAD must not return a body, got {body:?}");
        // GET on the same route DOES return the body, of that exact length.
        let get = String::from_utf8(state.respond("GET", "/metrics")).unwrap();
        let (_gh, gbody) = get.split_once("\r\n\r\n").unwrap();
        assert_eq!(gbody.len(), cl);
    }

    #[test]
    fn readyz_flips_with_the_ready_flag() {
        let state = test_state();
        let before = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        state.set_ready(true);
        let after = String::from_utf8(state.respond("GET", "/readyz")).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
    }

    #[test]
    fn unknown_path_is_404() {
        let resp = String::from_utf8(test_state().respond("GET", "/nope")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"), "{resp}");
    }

    #[test]
    fn root_serves_html_with_the_security_headers() {
        let resp = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/html; charset=utf-8"),
            "{resp}"
        );
        // The strict UI security headers are present.
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "missing CSP: {resp}"
        );
        assert!(resp.contains("X-Content-Type-Options: nosniff"), "{resp}");
        assert!(resp.contains("X-Frame-Options: DENY"), "{resp}");
        assert!(resp.contains("Referrer-Policy: no-referrer"), "{resp}");
        // The existing Connection: close is still emitted.
        assert!(resp.contains("Connection: close"), "{resp}");
        // A known marker from the dashboard shell is in the body, and it links
        // the SEPARATE css/js (so the CSP needs no 'unsafe-inline').
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(body.contains("IronCache Console"), "{body}");
        assert!(body.contains("/app.css"), "{body}");
        assert!(body.contains("/app.js"), "{body}");
    }

    #[test]
    fn root_serves_the_login_panel_markup() {
        // The sign-in affordance (UI auth, follow-up to #360) is served as part
        // of the static shell. Assert the served bytes carry the login element
        // ids and the password field (no browser, so we check the served body).
        let resp = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(body.contains("id=\"login-panel\""), "{body}");
        assert!(body.contains("id=\"login-token\""), "{body}");
        assert!(body.contains("id=\"login-submit\""), "{body}");
        assert!(body.contains("id=\"logout-submit\""), "{body}");
        assert!(body.contains("type=\"password\""), "{body}");
        // No inline event handlers in the served markup (CSP forbids them).
        assert!(!body.contains("onclick"), "{body}");
        assert!(!body.contains("onsubmit"), "{body}");
    }

    #[test]
    fn app_js_is_served_as_javascript() {
        let resp = String::from_utf8(test_state().respond("GET", "/app.js")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: application/javascript; charset=utf-8"),
            "{resp}"
        );
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
        // The served script is auth-aware: it sends a Bearer token from
        // sessionStorage and wires the controls via addEventListener (no inline
        // onclick / no localStorage), and still uses no innerHTML sink.
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(body.contains("Authorization"), "{body}");
        assert!(body.contains("Bearer "), "{body}");
        assert!(body.contains("sessionStorage"), "{body}");
        assert!(body.contains("addEventListener"), "{body}");
        assert!(!body.contains("localStorage"), "{body}");
        assert!(!body.contains(".innerHTML"), "{body}");
        assert!(!body.contains("onclick"), "{body}");
    }

    #[test]
    fn app_css_is_served_as_css() {
        let resp = String::from_utf8(test_state().respond("GET", "/app.css")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/css; charset=utf-8"),
            "{resp}"
        );
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
    }

    #[test]
    fn fonts_css_is_served_as_css_with_the_security_headers() {
        let resp = String::from_utf8(test_state().respond("GET", "/assets/fonts.css")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(
            resp.contains("Content-Type: text/css; charset=utf-8"),
            "{resp}"
        );
        // The self-hosted fonts carry the SAME strict UI security headers.
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
        assert!(resp.contains("X-Content-Type-Options: nosniff"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(body.contains("@font-face"), "{body}");
    }

    #[test]
    fn woff2_fonts_serve_as_binary_with_font_woff2_type() {
        for path in [
            "/assets/fonts/hanken-grotesk.woff2",
            "/assets/fonts/jetbrains-mono.woff2",
        ] {
            let resp = String::from_utf8_lossy(&test_state().respond("GET", path)).into_owned();
            assert!(resp.starts_with("HTTP/1.1 200 OK"), "{path}: {resp}");
            assert!(resp.contains("Content-Type: font/woff2"), "{path}: {resp}");
            // The strict UI security headers apply to the font binaries too.
            assert!(
                resp.contains("Content-Security-Policy: default-src 'self'"),
                "{path}: {resp}"
            );
            // The body is the raw woff2 (wOF2 magic), not re-encoded.
            let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
            assert!(body.starts_with("wOF2"), "{path}: body must be raw woff2");
        }
    }

    #[test]
    fn head_on_a_woff2_has_headers_and_correct_length_but_no_body() {
        let raw = test_state().respond("HEAD", "/assets/fonts/hanken-grotesk.woff2");
        let resp = String::from_utf8_lossy(&raw).into_owned();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(resp.contains("Content-Type: font/woff2"), "{resp}");
        let (header, body) = resp.split_once("\r\n\r\n").unwrap();
        let cl: usize = header
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // HEAD: the Content-Length reflects the GET body, but no body bytes follow.
        assert_eq!(cl, crate::assets::FONT_HANKEN_GROTESK_WOFF2.len());
        assert!(body.is_empty(), "HEAD must not return a body");
    }

    #[test]
    fn index_served_bytes_are_csp_clean() {
        // The served HTML must be CSP-clean: no inline style= attribute and no
        // inline on*= event handler (all styling is by class, all behavior is
        // wired with addEventListener). It also still links the separate assets.
        let resp = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        assert!(
            !body.contains(" style="),
            "served index must have no inline style="
        );
        assert!(
            !body.contains(" onclick"),
            "served index must have no onclick"
        );
        assert!(
            !body.contains(" onsubmit"),
            "served index must have no onsubmit"
        );
        assert!(
            !body.contains(" onload"),
            "served index must have no onload"
        );
        assert!(
            !body.contains(" onerror"),
            "served index must have no onerror"
        );
        assert!(body.contains("/app.css"), "{body}");
        assert!(body.contains("/app.js"), "{body}");
    }

    #[test]
    fn head_on_root_has_the_headers_but_no_body() {
        let resp = String::from_utf8(test_state().respond("HEAD", "/")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        // The headers (incl. CSP and a non-zero Content-Length) are present.
        assert!(
            resp.contains("Content-Security-Policy: default-src 'self'"),
            "{resp}"
        );
        let (header, body) = resp.split_once("\r\n\r\n").unwrap();
        let cl: usize = header
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            cl > 0,
            "HEAD Content-Length should match the GET body length"
        );
        // ...but no body bytes follow (RFC 9110).
        assert!(body.is_empty(), "HEAD must not return a body, got {body:?}");
        // The GET on `/` returns a body of exactly that length.
        let get = String::from_utf8(test_state().respond("GET", "/")).unwrap();
        let (_gh, gbody) = get.split_once("\r\n\r\n").unwrap();
        assert_eq!(gbody.len(), cl);
    }

    #[test]
    fn metrics_response_is_unchanged_by_the_header_extension() {
        // The probe/metrics responses must NOT carry the UI security headers (the
        // header-block extension is opt-in via `ui_response`), and the existing
        // headers are intact.
        let resp = String::from_utf8(test_state().respond("GET", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "{resp}");
        assert!(!resp.contains("Content-Security-Policy"), "{resp}");
        assert!(!resp.contains("X-Frame-Options"), "{resp}");
        assert!(resp.contains("Connection: close"), "{resp}");
    }

    /// `/api/health` is served through the bounded responder, returns JSON, and
    /// does not require a polled topology.
    #[tokio::test]
    async fn api_health_is_json_without_a_poll() {
        let state = test_state();
        state.set_live(true);
        let resp =
            String::from_utf8(state.respond_async("GET", "/api/health", None).await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(resp.contains("Content-Type: application/json"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["live"], true);
        assert_eq!(v["ready"], false);
    }

    /// SECURITY (#369): every `/api/*` JSON response carries `nosniff` +
    /// `no-store`, so the sensitive data is neither content-sniffed nor cached.
    /// Checked across success, 503-before-poll, 401, and 405.
    #[tokio::test]
    async fn api_responses_carry_nosniff_and_no_store() {
        // 200 (health, no poll needed).
        let state = test_state();
        state.set_live(true);
        let ok = String::from_utf8(state.respond_async("GET", "/api/health", None).await).unwrap();
        assert!(ok.contains("X-Content-Type-Options: nosniff"), "{ok}");
        assert!(ok.contains("Cache-Control: no-store"), "{ok}");

        // 503 (a data route before the first poll) still carries them.
        let unavailable =
            String::from_utf8(state.respond_async("GET", "/api/cluster", None).await).unwrap();
        assert!(unavailable.starts_with("HTTP/1.1 503"), "{unavailable}");
        assert!(
            unavailable.contains("X-Content-Type-Options: nosniff"),
            "{unavailable}"
        );
        assert!(
            unavailable.contains("Cache-Control: no-store"),
            "{unavailable}"
        );

        // 401 (a privileged route, enforcing, no token) carries them.
        let enforced = state_with_auth(AuthPolicy::resolve(Some("read-tok"), None, true));
        publish_empty_topology(&enforced).await;
        let unauthorized =
            String::from_utf8(enforced.respond_async("GET", "/api/nodes", None).await).unwrap();
        assert!(unauthorized.starts_with("HTTP/1.1 401"), "{unauthorized}");
        assert!(
            unauthorized.contains("X-Content-Type-Options: nosniff"),
            "{unauthorized}"
        );
        assert!(
            unauthorized.contains("Cache-Control: no-store"),
            "{unauthorized}"
        );

        // 405 (a non-GET to an /api/* route) carries them too.
        let method_not_allowed =
            String::from_utf8(state.respond_async("POST", "/api/cluster", None).await).unwrap();
        assert!(
            method_not_allowed.starts_with("HTTP/1.1 405"),
            "{method_not_allowed}"
        );
        assert!(
            method_not_allowed.contains("X-Content-Type-Options: nosniff"),
            "{method_not_allowed}"
        );
        assert!(
            method_not_allowed.contains("Cache-Control: no-store"),
            "{method_not_allowed}"
        );
    }

    /// SECURITY (#369): the API headers are scoped to `/api/*` ONLY. The probe and
    /// metrics responses must NOT carry `Cache-Control: no-store` (the header
    /// extension is opt-in via `api_response` / `ui_response`).
    #[test]
    fn probe_and_metrics_responses_have_no_cache_control() {
        let state = test_state();
        state.set_live(true);
        for path in ["/livez", "/readyz", "/metrics"] {
            let resp = String::from_utf8(state.respond("GET", path)).unwrap();
            assert!(
                !resp.contains("Cache-Control"),
                "{path} must not carry Cache-Control: {resp}"
            );
        }
    }

    /// A data route is `503` JSON before the first poll, then `200` after a
    /// topology is published into the shared holder.
    #[tokio::test]
    async fn api_cluster_is_503_before_poll_then_200_after() {
        use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};
        let state = test_state();
        let before =
            String::from_utf8(state.respond_async("GET", "/api/cluster", None).await).unwrap();
        assert!(before.starts_with("HTTP/1.1 503"), "{before}");
        assert!(before.contains("application/json"), "{before}");
        let (_h, body) = before.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(v["error"].is_string(), "{body}");

        let topo = Topology {
            mode: TopologyMode::Standalone,
            nodes: vec![NodeSnapshot {
                addr: "10.0.0.1:6379".to_owned(),
                reachable: true,
                error: None,
                info: None,
                slowlog: Vec::new(),
                slowlog_error: None,
                clients: Vec::new(),
                clients_error: None,
                fetched_unixtime: 42,
            }],
            cluster: None,
            fetched_unixtime: 42,
        };
        *state.topology().write().await = Some(topo);
        let after =
            String::from_utf8(state.respond_async("GET", "/api/cluster", None).await).unwrap();
        assert!(after.starts_with("HTTP/1.1 200 OK"), "{after}");
        let (_h, body) = after.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["mode"], "standalone");
        assert_eq!(v["nodes_total"], 1);
    }

    /// An unknown `/api/*` endpoint is `404` JSON, and a non-GET to `/api/*` is
    /// `405`.
    #[tokio::test]
    async fn api_unknown_is_404_and_post_is_405() {
        let state = test_state();
        // A topology so we are past the 503-before-poll gate.
        *state.topology().write().await = Some(crate::snapshot::Topology {
            mode: crate::snapshot::TopologyMode::Standalone,
            nodes: Vec::new(),
            cluster: None,
            fetched_unixtime: 1,
        });
        let nf = String::from_utf8(state.respond_async("GET", "/api/bogus", None).await).unwrap();
        assert!(nf.starts_with("HTTP/1.1 404 Not Found"), "{nf}");
        assert!(nf.contains("application/json"), "{nf}");
        let post =
            String::from_utf8(state.respond_async("POST", "/api/cluster", None).await).unwrap();
        assert!(post.starts_with("HTTP/1.1 405"), "{post}");
    }

    /// `/api/openapi.json` is served and parses as JSON.
    #[tokio::test]
    async fn api_openapi_is_valid_json() {
        let state = test_state();
        let resp =
            String::from_utf8(state.respond_async("GET", "/api/openapi.json", None).await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["openapi"], "3.0.3");
    }

    #[test]
    fn non_get_is_405() {
        let resp = String::from_utf8(test_state().respond("POST", "/metrics")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 405"), "{resp}");
    }

    #[test]
    fn query_string_is_stripped() {
        let state = test_state();
        state.set_live(true);
        let resp = String::from_utf8(state.respond("GET", "/livez?foo=bar")).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }

    #[test]
    fn request_line_parse_incomplete_then_complete() {
        assert!(parse_request_line(b"GET /metrics HTTP/1.1").is_none());
        let (m, p) = parse_request_line(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/metrics");
        let (m, p) = parse_request_line(b"GET /livez HTTP/1.1\n").unwrap();
        assert_eq!(m, "GET");
        assert_eq!(p, "/livez");
    }

    /// A slow-drip client that sends a partial request line then stalls is
    /// dropped by the whole-request deadline, not held to the size cap.
    #[tokio::test]
    async fn slow_drip_request_is_dropped_within_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /metr").await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(c);
        });
        let (stream, _peer) = listener.accept().await.unwrap();
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            serve_conn_with_deadline(stream, test_state(), Duration::from_millis(200)),
        )
        .await;
        assert!(
            served.is_ok(),
            "stalled connection must be dropped at the deadline"
        );
        client.abort();
    }

    /// A complete request within the deadline gets the normal response.
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
        let state = test_state();
        state.set_live(true);
        serve_conn_with_deadline(stream, state, Duration::from_secs(5)).await;
        let body = client.await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    }

    /// The auth/RBAC gate (#360), exercised through `respond_async` so the routing,
    /// the tier map, and the posture all run together.

    #[tokio::test]
    async fn enforce_privileged_route_without_token_is_401_json() {
        let state = state_with_auth(AuthPolicy::resolve(Some("read-tok"), None, true));
        publish_empty_topology(&state).await;
        let resp = String::from_utf8(state.respond_async("GET", "/api/nodes", None).await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized"), "{resp}");
        assert!(resp.contains("application/json"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(v["error"].is_string(), "{body}");
    }

    #[tokio::test]
    async fn enforce_open_route_needs_no_token() {
        let state = state_with_auth(AuthPolicy::resolve(Some("read-tok"), None, true));
        publish_empty_topology(&state).await;
        // /api/cluster is OPEN: served even with no token while enforcing.
        let resp =
            String::from_utf8(state.respond_async("GET", "/api/cluster", None).await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        // /api/health is OPEN too (and needs no topology).
        let resp =
            String::from_utf8(state.respond_async("GET", "/api/health", None).await).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }

    #[tokio::test]
    async fn enforce_read_token_grants_privileged() {
        let state = state_with_auth(AuthPolicy::resolve(
            Some("read-tok"),
            Some("admin-tok"),
            true,
        ));
        publish_empty_topology(&state).await;
        let resp = String::from_utf8(
            state
                .respond_async("GET", "/api/nodes", Some("Bearer read-tok"))
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }

    #[tokio::test]
    async fn enforce_wrong_token_is_401() {
        let state = state_with_auth(AuthPolicy::resolve(
            Some("read-tok"),
            Some("admin-tok"),
            true,
        ));
        publish_empty_topology(&state).await;
        let resp = String::from_utf8(
            state
                .respond_async("GET", "/api/slowlog", Some("Bearer WRONG"))
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized"), "{resp}");
    }

    #[tokio::test]
    async fn exposed_no_token_serves_open_blocks_privileged() {
        // Non-loopback bind, no token: OPEN served, privileged 401.
        let state = state_with_auth(AuthPolicy::resolve(None, None, false));
        publish_empty_topology(&state).await;
        let open =
            String::from_utf8(state.respond_async("GET", "/api/cluster", None).await).unwrap();
        assert!(open.starts_with("HTTP/1.1 200 OK"), "{open}");
        let priv_resp =
            String::from_utf8(state.respond_async("GET", "/api/clients", None).await).unwrap();
        assert!(
            priv_resp.starts_with("HTTP/1.1 401 Unauthorized"),
            "{priv_resp}"
        );
    }

    #[tokio::test]
    async fn trailing_slash_on_privileged_route_is_not_open_bypass() {
        // `/api/nodes/` (trailing slash) must be gated like the privileged node
        // routes, not slip into the Open default. With no token on an exposed
        // bind it is 401, never served.
        let state = state_with_auth(AuthPolicy::resolve(None, None, false));
        publish_empty_topology(&state).await;
        for p in ["/api/nodes/", "/api/cluster/", "/api/bogus"] {
            let resp = String::from_utf8(state.respond_async("GET", p, None).await).unwrap();
            assert!(
                resp.starts_with("HTTP/1.1 401 Unauthorized"),
                "{p} must be gated: {resp}"
            );
        }
    }

    #[test]
    fn authorization_header_is_extracted_case_insensitively() {
        let req = b"GET /api/nodes HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok\r\n\r\n";
        assert_eq!(authorization_header(req).as_deref(), Some("Bearer tok"));
        // Case-insensitive field name; value trimmed.
        let req = b"GET /api/nodes HTTP/1.1\r\nauthorization:   Bearer tok2  \r\n\r\n";
        assert_eq!(authorization_header(req).as_deref(), Some("Bearer tok2"));
        // Absent header.
        let req = b"GET /api/nodes HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(authorization_header(req), None);
        // A header-looking line in the BODY is not picked up (only the head is
        // scanned).
        let req = b"GET /api/nodes HTTP/1.1\r\nHost: x\r\n\r\nAuthorization: Bearer body\r\n";
        assert_eq!(authorization_header(req), None);
    }

    #[test]
    fn header_block_complete_detects_terminator() {
        assert!(!header_block_complete(
            b"GET /api/nodes HTTP/1.1\r\nHost: x\r\n"
        ));
        assert!(header_block_complete(
            b"GET /api/nodes HTTP/1.1\r\nHost: x\r\n\r\n"
        ));
        assert!(header_block_complete(b"GET / HTTP/1.1\n\n"));
    }

    #[test]
    fn header_block_end_and_content_length_parse() {
        let req = b"POST /api/config HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";
        let end = header_block_end(req).unwrap();
        assert_eq!(&req[end..], b"hello");
        assert_eq!(content_length(req), Some(5));
        // Absent Content-Length -> None.
        let req2 = b"GET /api/config HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(content_length(req2), None);
        // Case-insensitive field name.
        let req3 = b"POST /x HTTP/1.1\r\ncontent-length: 3\r\n\r\nabc";
        assert_eq!(content_length(req3), Some(3));
        // A non-numeric Content-Length is None (treated as no body), never a panic.
        let req4 = b"POST /x HTTP/1.1\r\nContent-Length: NaN\r\n\r\n";
        assert_eq!(content_length(req4), None);
    }

    #[test]
    fn is_management_read_routes_the_right_paths() {
        for p in [
            "/api/config",
            "/api/keys",
            "/api/keys/foo",
            "/api/pubsub/channels",
            "/api/acl",
            "/api/persistence",
        ] {
            assert!(is_management_read(p), "{p} should be a management read");
        }
        // Monitoring reads are NOT management reads (served from the topology).
        for p in ["/api/cluster", "/api/nodes", "/api/slowlog", "/api/health"] {
            assert!(
                !is_management_read(p),
                "{p} should not be a management read"
            );
        }
    }

    #[test]
    fn parse_body_rejects_malformed_json_with_400_not_panic() {
        // Malformed JSON -> a 400 ApiResponse, never a panic.
        let bad: Result<crate::manage::ConfigSetBody, _> = parse_body(b"{not json");
        let resp = bad.unwrap_err();
        assert_eq!(resp.status, 400);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert!(v["error"].is_string());
        // Wrong-shape JSON (missing required field) -> 400 too.
        let wrong: Result<crate::manage::ConfigSetBody, _> = parse_body(b"{\"param\":\"x\"}");
        assert_eq!(wrong.unwrap_err().status, 400);
        // Well-formed -> Ok.
        let ok: Result<crate::manage::ConfigSetBody, _> =
            parse_body(b"{\"param\":\"maxmemory\",\"value\":\"100mb\"}");
        assert!(ok.is_ok());
    }

    // ---- management tier gate + dispatch (#361) -----------------------------

    /// A state whose node_access points at `addr`, with an explicit auth policy and
    /// a published (empty) topology, so the management gate + dispatch are exercised.
    fn state_with_node(auth: AuthPolicy, addr: &str) -> ConsoleHttpState {
        let access = crate::node::NodeAccess {
            addr: addr.to_owned(),
            tls: None,
            auth: None,
            connect_timeout: Duration::from_secs(2),
            op_timeout: Duration::from_secs(2),
        };
        let s = ConsoleHttpState::with_topology_and_auth(
            Arc::new(ConsoleMetrics::new()),
            new_topology_holder(),
            auth,
        )
        .with_node_access(Some(Arc::new(access)));
        s.set_live(true);
        s
    }

    /// A POST/DELETE to a management route WITHOUT the admin token is blocked at the
    /// gate (401 with no token, 403 with a read-only token) and NEVER reaches the
    /// node (we point at a dead addr, so reaching it would be a 502).
    #[tokio::test]
    async fn mutation_without_admin_is_blocked_before_the_node() {
        // Enforcing with a read token only: a POST is Admin, so a read token is 403.
        let state = state_with_node(
            AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), true),
            "127.0.0.1:1", // dead: a 502 here would prove the gate failed.
        );
        // No token -> 401.
        let resp = String::from_utf8(
            state
                .respond_async_with_body("POST", "/api/config", None, b"{}")
                .await,
        )
        .unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 401"),
            "no token must be 401: {resp}"
        );
        // A read token on a write -> 403 (valid token, insufficient tier).
        let resp = String::from_utf8(
            state
                .respond_async_with_body("POST", "/api/config", Some("Bearer read-tok"), b"{}")
                .await,
        )
        .unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 403"),
            "read token on a write must be 403: {resp}"
        );
        // A DELETE on a key is also Admin: no token -> 401.
        let resp = String::from_utf8(
            state
                .respond_async_with_body("DELETE", "/api/keys/foo", None, b"")
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 401"), "{resp}");
    }

    /// A management read with an INSUFFICIENT token is gated too: /api/acl is Admin,
    /// so a read token gets 403; /api/config is PrivilegedRead, so a read token is
    /// allowed (and would reach the node).
    #[tokio::test]
    async fn acl_read_requires_admin_config_read_allows_read_token() {
        let state = state_with_node(
            AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), true),
            "127.0.0.1:1",
        );
        // ACL read with the read token -> 403 (Admin-only read).
        let resp = String::from_utf8(
            state
                .respond_async_with_body("GET", "/api/acl", Some("Bearer read-tok"), b"")
                .await,
        )
        .unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 403"),
            "acl read must need admin: {resp}"
        );
        // Config read with the read token -> allowed by the gate; reaches the dead
        // node and returns 502 (proving it passed the gate, not 401/403).
        let resp = String::from_utf8(
            state
                .respond_async_with_body("GET", "/api/config", Some("Bearer read-tok"), b"")
                .await,
        )
        .unwrap();
        assert!(
            resp.starts_with("HTTP/1.1 502"),
            "config read must pass the gate and hit the node: {resp}"
        );
    }

    /// A stub RESP node that answers a scripted sequence of replies (one per
    /// command it reads), so the management dispatch can be exercised end to end.
    async fn spawn_stub_node(replies: Vec<&'static [u8]>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _peer) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 4096];
            for reply in replies {
                let _ = stream_read(&mut sock, &mut chunk).await;
                let _ = sock.write_all(reply).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        addr
    }

    async fn stream_read(sock: &mut tokio::net::TcpStream, buf: &mut [u8]) -> usize {
        sock.read(buf).await.unwrap_or(0)
    }

    /// POST /api/config with the admin token issues CONFIG SET and returns {ok}.
    #[tokio::test]
    async fn config_set_with_admin_reaches_node_and_returns_ok() {
        let addr = spawn_stub_node(vec![b"+OK\r\n"]).await;
        let state = state_with_node(AuthPolicy::resolve(None, Some("admin-tok"), true), &addr);
        let resp = String::from_utf8(
            state
                .respond_async_with_body(
                    "POST",
                    "/api/config",
                    Some("Bearer admin-tok"),
                    b"{\"param\":\"maxmemory\",\"value\":\"100mb\"}",
                )
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["ok"], true);
    }

    /// POST /api/config with an INVALID param (whitespace) is a 400 and does NOT
    /// reach the node (the stub would still be waiting on its first read).
    #[tokio::test]
    async fn config_set_invalid_param_is_400() {
        let addr = spawn_stub_node(vec![b"+OK\r\n"]).await;
        let state = state_with_node(AuthPolicy::resolve(None, Some("admin-tok"), true), &addr);
        let resp = String::from_utf8(
            state
                .respond_async_with_body(
                    "POST",
                    "/api/config",
                    Some("Bearer admin-tok"),
                    b"{\"param\":\"max memory\",\"value\":\"1\"}",
                )
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
    }

    /// GET /api/keys (SCAN) with the read token returns the {cursor, keys} shape.
    /// The stub answers SCAN then TYPE+TTL for the one returned key.
    #[tokio::test]
    async fn keys_scan_returns_the_scan_shape() {
        let addr = spawn_stub_node(vec![
            // SCAN 0 MATCH * COUNT 100 -> ["0", ["user:1"]]
            b"*2\r\n$1\r\n0\r\n*1\r\n$6\r\nuser:1\r\n",
            // TYPE user:1 -> +string
            b"+string\r\n",
            // TTL user:1 -> :-1
            b":-1\r\n",
        ])
        .await;
        let state = state_with_node(AuthPolicy::resolve(Some("read-tok"), None, true), &addr);
        let resp = String::from_utf8(
            state
                .respond_async_with_body("GET", "/api/keys?pattern=*", Some("Bearer read-tok"), b"")
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["cursor"], "0");
        assert_eq!(v["keys"][0]["key"], "user:1");
        assert_eq!(v["keys"][0]["type"], "string");
        assert_eq!(v["keys"][0]["ttl"], -1);
    }

    /// DELETE /api/keys/{k} requires admin: a read token is 403; the admin token
    /// reaches the node and returns the deleted count.
    #[tokio::test]
    async fn key_delete_requires_admin_then_works() {
        let addr = spawn_stub_node(vec![b":1\r\n"]).await;
        let state = state_with_node(
            AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), true),
            &addr,
        );
        // Read token -> 403, never reaches the node.
        let denied = String::from_utf8(
            state
                .respond_async_with_body("DELETE", "/api/keys/user:1", Some("Bearer read-tok"), b"")
                .await,
        )
        .unwrap();
        assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");
        // Admin token -> DEL reaches the node, returns {deleted:1}.
        let ok = String::from_utf8(
            state
                .respond_async_with_body(
                    "DELETE",
                    "/api/keys/user:1",
                    Some("Bearer admin-tok"),
                    b"",
                )
                .await,
        )
        .unwrap();
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "{ok}");
        let (_h, body) = ok.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["deleted"], 1);
    }

    /// The arbitrary command runner renders a node -ERR into reply.kind=error (a
    /// 200 with ok:false), so the console shows the node's response.
    #[tokio::test]
    async fn command_runner_renders_node_error_reply() {
        let addr = spawn_stub_node(vec![b"-ERR unknown command 'BOGUS'\r\n"]).await;
        let state = state_with_node(AuthPolicy::resolve(None, Some("admin-tok"), true), &addr);
        let resp = String::from_utf8(
            state
                .respond_async_with_body(
                    "POST",
                    "/api/command",
                    Some("Bearer admin-tok"),
                    b"{\"args\":[\"BOGUS\"]}",
                )
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        let (_h, body) = resp.split_once("\r\n\r\n").unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["reply"]["kind"], "error");
        assert!(
            v["reply"]["value"]
                .as_str()
                .unwrap()
                .contains("unknown command")
        );
    }

    /// An empty command argv is a 400 (bounded), never a panic, and never reaches
    /// the node.
    #[tokio::test]
    async fn command_runner_rejects_empty_argv() {
        let addr = spawn_stub_node(vec![b"+OK\r\n"]).await;
        let state = state_with_node(AuthPolicy::resolve(None, Some("admin-tok"), true), &addr);
        let resp = String::from_utf8(
            state
                .respond_async_with_body(
                    "POST",
                    "/api/command",
                    Some("Bearer admin-tok"),
                    b"{\"args\":[]}",
                )
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
    }

    /// Management with NO node access configured (no seed) is a 503, even with the
    /// admin token (the gate passes, the dispatch reports unavailable).
    #[tokio::test]
    async fn management_without_seed_is_503() {
        let state = state_with_auth(AuthPolicy::resolve(None, Some("admin-tok"), true));
        let resp = String::from_utf8(
            state
                .respond_async_with_body("GET", "/api/config", Some("Bearer admin-tok"), b"")
                .await,
        )
        .unwrap();
        assert!(resp.starts_with("HTTP/1.1 503"), "{resp}");
    }

    /// An over-cap request body is a 413 from the read phase, never an unbounded
    /// alloc, exercised over a real socket.
    #[tokio::test]
    async fn oversized_body_is_413_over_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = state_with_auth(AuthPolicy::resolve(None, Some("admin-tok"), true));
        let serving = state.clone();
        tokio::spawn(async move {
            accept_loop(listener, serving).await;
        });
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        // A declared body far over MAX_REQUEST_BYTES; send a big chunk so the read
        // loop trips the cap.
        let big = vec![b'a'; MAX_REQUEST_BYTES + 1024];
        let head = format!(
            "POST /api/config HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer admin-tok\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            big.len()
        );
        c.write_all(head.as_bytes()).await.unwrap();
        c.write_all(&big).await.unwrap();
        let mut raw = Vec::new();
        let _ = c.read_to_end(&mut raw).await;
        let resp = String::from_utf8_lossy(&raw);
        assert!(resp.starts_with("HTTP/1.1 413"), "{resp}");
    }

    /// End-to-end over a real socket: an enforcing console returns 401 without a
    /// token and 200 with the right Bearer token on a privileged route.
    #[tokio::test]
    async fn enforce_over_tcp_401_then_200_with_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = state_with_auth(AuthPolicy::resolve(Some("the-tok"), None, true));
        publish_empty_topology(&state).await;
        let serving = state.clone();
        tokio::spawn(async move {
            accept_loop(listener, serving).await;
        });

        // No token -> 401.
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET /api/nodes HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        c.read_to_end(&mut raw).await.unwrap();
        let resp = String::from_utf8_lossy(&raw);
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized"), "{resp}");

        // Right token -> 200.
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(
            b"GET /api/nodes HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer the-tok\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        let mut raw = Vec::new();
        c.read_to_end(&mut raw).await.unwrap();
        let resp = String::from_utf8_lossy(&raw);
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    }
}
