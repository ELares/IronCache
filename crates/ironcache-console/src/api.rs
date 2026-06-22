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

use crate::snapshot::{NodeSnapshot, Topology, TopologyMode};

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
    fn ok<T: Serialize>(value: &T) -> Self {
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
    fn not_found(message: &str) -> Self {
        ApiResponse {
            status: 404,
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
    totals: ClusterTotals,
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

/// Whether `path` (already query-stripped) is in the `/api/` namespace, so the
/// HTTP layer can route it here. `/api/openapi.json` is included.
#[must_use]
pub fn is_api_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

/// Handle one API request. `path` is the query-stripped request path; `topology`
/// is the latest polled topology (`None` before the first poll). Returns an
/// [`ApiResponse`] (status + JSON body) for the HTTP layer to frame.
///
/// Routing:
/// * `/api/health` and `/api/openapi.json` do not need a topology.
/// * every OTHER `/api/*` data route returns `503` before the first poll.
#[must_use]
pub fn handle(path: &str, topology: Option<&Topology>, ctx: &ApiContext<'_>) -> ApiResponse {
    // These two do not depend on a polled topology.
    match path {
        "/api/health" => return health(ctx),
        "/api/openapi.json" => {
            return ApiResponse {
                status: 200,
                body: openapi_document().to_owned(),
            };
        }
        _ => {}
    }

    // Every data route needs a topology; before the first poll, 503.
    let Some(topo) = topology else {
        return ApiResponse::not_polled();
    };

    match path {
        "/api/cluster" => cluster(topo),
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
fn cluster(topo: &Topology) -> ApiResponse {
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
    }
    ApiResponse::ok(&ClusterOverview {
        mode: topo.mode,
        nodes_total,
        nodes_reachable,
        last_poll_unixtime: topo.fetched_unixtime,
        totals,
    })
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

/// Extract a `name=<u64>` field from a comma-separated keyspace value
/// (`keys=10,expires=2,avg_ttl=0`).
fn field_in(value: &str, name: &str) -> Option<u64> {
    let prefix = format!("{name}=");
    value
        .split(',')
        .find_map(|part| part.trim().strip_prefix(&prefix))
        .and_then(|n| n.trim().parse::<u64>().ok())
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
    "version": "1.0.0",
    "description": "Read-only monitoring API over a polled IronCache deployment. Unauthenticated today; to be placed behind the auth/RBAC tier (#360) and VPN-locked exposure (#369)."
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
    "/api/openapi.json": {
      "get": {
        "summary": "This OpenAPI document.",
        "responses": {
          "200": {
            "description": "The OpenAPI 3.0 document."
          }
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
          "expired_keys": { "type": "integer", "format": "int64" }
        }
      },
      "ClusterOverview": {
        "type": "object",
        "properties": {
          "mode": { "type": "string", "enum": ["standalone", "clustered"] },
          "nodes_total": { "type": "integer" },
          "nodes_reachable": { "type": "integer" },
          "last_poll_unixtime": { "type": "integer", "format": "int64" },
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
        assert_eq!(v["totals"]["keys"], 15);
        assert_eq!(v["totals"]["used_memory"], 1024);
        assert_eq!(v["totals"]["connected_clients"], 3);
        assert_eq!(v["totals"]["keyspace_hits"], 80);
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
            "/api/openapi.json",
        ] {
            assert!(paths.contains_key(p), "openapi missing path {p}");
        }
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
}
