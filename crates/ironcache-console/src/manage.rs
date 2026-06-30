// SPDX-License-Identifier: MIT OR Apache-2.0
//! The console's node-level MANAGEMENT layer (issue #361, single-node subset).
//!
//! This module turns the console from a read-only monitor into a bounded WRITE
//! surface against ONE node: it issues `CONFIG SET`, key CRUD, an arbitrary
//! command console, pub/sub publish, ACL user management, and a persistence save,
//! plus the sensitive management READS (`CONFIG GET`, the SCAN key browser, a key
//! inspector, the channel list, persistence status, and the ACL list).
//!
//! ## Where the security boundary lives
//!
//! Every MUTATION is `Admin`-tier and is gated SERVER-SIDE in [`crate::http`]
//! BEFORE any handler here runs (see [`crate::auth::route_tier_for_method`]): a
//! `POST`/`DELETE` can never inherit a read route's lower tier. The sensitive
//! reads are `PrivilegedRead` (or `Admin` for the ACL list). The console executes
//! everything over its existing RESP [`NodeClient`], whose AUTH is the node's own
//! least-privilege ACL user, so the node ACL is the ultimate bound (defense in
//! depth): even the arbitrary-command console cannot exceed what that ACL user may
//! do. A mutation is AUDIT-LOGGED via tracing (the action, the target, the result,
//! and the authenticated tier); secrets/tokens/passwords are NEVER logged.
//!
//! ## Bounds and panic-freedom
//!
//! The request body is parsed by the HTTP layer under the existing
//! `MAX_REQUEST_BYTES` cap (an oversized body is a `413`, never an unbounded
//! alloc), and malformed JSON is a `400` (never a panic). The arbitrary command is
//! bounded (a non-empty argv, a per-arg length cap, and a total-bytes cap). The
//! SCAN browser caps the page size and the pattern length and uses `SCAN` (never
//! `KEYS`). Every node operation inherits [`NodeClient`]'s per-op timeout, so a
//! hung node surfaces a `502` promptly rather than hanging the responder.
//!
//! ## Determinism (ADR-0003)
//!
//! No clock, no RNG here: this module is request -> RESP -> JSON. The only
//! nondeterminism is the bounded node I/O, which goes through the runtime timer
//! seam inside [`NodeClient`].

use serde::{Deserialize, Serialize};

use crate::api::ApiResponse;
use crate::node::{NodeClient, NodeError};
use crate::resp::RespValue;

/// The default `SCAN COUNT` hint when the request omits `?count=`. Used by the
/// HTTP dispatch when the query has no `count` parameter.
pub(crate) const DEFAULT_SCAN_COUNT: u64 = 100;
/// The maximum keys returned in one SCAN page. The handler stops collecting once
/// this many keys are gathered (a SCAN can over-return, and each key costs a
/// `TYPE` + `TTL` round trip), so one page never fans out unboundedly.
const MAX_SCAN_KEYS: usize = 200;
/// The maximum `SCAN COUNT` hint a request may pass (clamped, not rejected) so a
/// huge `?count=` cannot ask the node to scan an enormous slice in one call.
const MAX_SCAN_COUNT: u64 = 1000;
/// The maximum byte length of a SCAN `MATCH` pattern (bounded so a pathological
/// pattern cannot be sent to the node).
const MAX_PATTERN_LEN: usize = 512;
/// The maximum element count read back when inspecting a collection key
/// (`LRANGE 0 N`, `HGETALL`-then-cap, `SMEMBERS`-then-cap, `ZRANGE 0 N`). A larger
/// collection is truncated and the inspector notes it.
const MAX_INSPECT_ELEMENTS: i64 = 256;
/// The maximum number of args in an arbitrary `/api/command` request.
const MAX_COMMAND_ARGS: usize = 64;
/// The maximum byte length of a single arbitrary-command arg.
const MAX_COMMAND_ARG_LEN: usize = 64 * 1024;
/// The maximum total byte length across all arbitrary-command args.
const MAX_COMMAND_TOTAL_LEN: usize = 256 * 1024;
/// The maximum byte length of a value written via `POST /api/keys/{k}`.
const MAX_VALUE_LEN: usize = 1024 * 1024;
/// The maximum byte length of a published pub/sub message.
const MAX_MESSAGE_LEN: usize = 256 * 1024;
/// The maximum number of ACL rule tokens in one `ACL SETUSER`.
const MAX_ACL_RULES: usize = 256;

/// A validation error for a management request body / parameter. Mapped to a
/// `400` JSON by the caller; never panics, never echoes a secret.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    /// A required field was empty / missing.
    #[error("{0}")]
    Empty(&'static str),
    /// A field carried a forbidden character (a space, CR, or LF in a token that
    /// must be a single line, etc.).
    #[error("{0}")]
    Forbidden(&'static str),
    /// A field was longer than its bound.
    #[error("{0}")]
    TooLong(&'static str),
    /// A numeric field could not be parsed / was out of range.
    #[error("{0}")]
    BadNumber(&'static str),
}

// ---- request bodies (deserialized from the POST/DELETE JSON) ----------------

/// `POST /api/config` body: a single `CONFIG SET <param> <value>`.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigSetBody {
    /// The config parameter (a plausible config token: no spaces/CR/LF).
    pub param: String,
    /// The new value (taken verbatim; sent as one RESP bulk arg).
    pub value: String,
}

/// `POST /api/keys/{k}` body: a string `SET`.
#[derive(Debug, Clone, Deserialize)]
pub struct KeySetBody {
    /// The value to `SET` (string only for v1; typed writes are a string SET).
    pub value: String,
}

/// `POST /api/keys/{k}/expire` body.
#[derive(Debug, Clone, Deserialize)]
pub struct ExpireBody {
    /// The TTL in seconds (a non-negative integer).
    pub seconds: i64,
}

/// `POST /api/command` body: an arbitrary command argv.
#[derive(Debug, Clone, Deserialize)]
pub struct CommandBody {
    /// The command and its args (non-empty; each bounded; total bounded).
    pub args: Vec<String>,
}

/// `POST /api/pubsub/publish` body.
#[derive(Debug, Clone, Deserialize)]
pub struct PublishBody {
    /// The channel to publish to.
    pub channel: String,
    /// The message payload.
    pub message: String,
}

/// `POST /api/acl/user` body: an `ACL SETUSER`.
#[derive(Debug, Clone, Deserialize)]
pub struct AclUserBody {
    /// The username (required; no CR/LF/space).
    pub username: String,
    /// The ACL rule tokens (each a single line; e.g. `on`, `>pass`, `~key:*`,
    /// `+get`). Bounded in count; each token must not carry CR/LF.
    #[serde(default)]
    pub rules: Vec<String>,
}

/// `POST /api/persistence/save` body.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SaveBody {
    /// Whether to `BGSAVE` (true, the default-safe async save) or block on `SAVE`
    /// (false). A blocking SAVE is rare and must be opted into explicitly.
    #[serde(default)]
    pub background: bool,
}

/// `POST /api/cluster/failover` body (#361): the typed destructive-confirmation. The
/// operator must echo the literal token [`FAILOVER_CONFIRM`] (`"FAILOVER"`) so a stray
/// or replayed POST cannot trigger a promotion. No options are accepted: the console
/// only ever issues a BARE `CLUSTER FAILOVER` (the safe, in-sync-gated form), never
/// `FORCE`/`TAKEOVER`, which the engine refuses anyway.
#[derive(Debug, Clone, Deserialize)]
pub struct FailoverBody {
    /// Must equal [`FAILOVER_CONFIRM`] or the request is a `400`.
    pub confirm: String,
}

/// `POST /api/cluster/meet` body (#361): add a node to the cluster (`CLUSTER MEET host
/// port`). Additive, so NO destructive-confirmation is required (the issue scopes typed
/// confirmation to FORGET/SETSLOT/FLUSH-class actions); the engine validates the
/// address and the leader path commits it.
#[derive(Debug, Clone, Deserialize)]
pub struct MeetBody {
    /// The advertised host of the node to add (a single bare token, no CR/LF).
    pub host: String,
    /// The advertised RESP port (1..=65535; `0` is rejected).
    pub port: u16,
}

/// `POST /api/cluster/forget` body (#361): remove a node from the cluster view
/// (`CLUSTER FORGET node-id`). DESTRUCTIVE, so `confirm` must ECHO the exact `node_id`
/// being forgotten (the "type the target to confirm" pattern): an operator cannot
/// forget the wrong node by a stray POST, and the engine still validates that the id
/// exists / is not self.
#[derive(Debug, Clone, Deserialize)]
pub struct ForgetBody {
    /// The 40-hex node id to forget (no CR/LF).
    pub node_id: String,
    /// Must equal `node_id` (trimmed) or the request is a `400`.
    pub confirm: String,
}

// ---- response bodies --------------------------------------------------------

/// `{ "ok": true }` for a mutation that the node answered `+OK` to.
#[derive(Debug, Clone, Serialize)]
struct OkResponse {
    ok: bool,
}

/// `{ "deleted": <n> }` for `DEL` / `ACL DELUSER`.
#[derive(Debug, Clone, Serialize)]
struct DeletedResponse {
    deleted: i64,
}

/// `{ "receivers": <n> }` for `PUBLISH`.
#[derive(Debug, Clone, Serialize)]
struct PublishResponse {
    receivers: i64,
}

/// One `CONFIG GET` row.
#[derive(Debug, Clone, Serialize)]
struct ConfigRow {
    param: String,
    value: String,
}

/// `GET /api/config` body: the sorted `[{param, value}]` rows.
#[derive(Debug, Clone, Serialize)]
struct ConfigResponse {
    params: Vec<ConfigRow>,
}

/// One key in a SCAN page.
#[derive(Debug, Clone, Serialize)]
struct ScanKey {
    key: String,
    /// The key TYPE (`string`/`list`/`hash`/`set`/`zset`/`stream`/`none`).
    r#type: String,
    /// The TTL in seconds: `-1` = no expiry, `-2` = the key is gone.
    ttl: i64,
}

/// `GET /api/keys` body: a SCAN page plus the continuation cursor.
#[derive(Debug, Clone, Serialize)]
struct ScanResponse {
    /// The next SCAN cursor (`"0"` when the iteration is complete).
    cursor: String,
    keys: Vec<ScanKey>,
}

/// `GET /api/keys/{k}` body: a single key's type, TTL, and (capped) value.
#[derive(Debug, Clone, Serialize)]
struct KeyDetail {
    key: String,
    r#type: String,
    ttl: i64,
    /// The rendered value: a string for `string`, or the (capped) element list for
    /// a collection, each element a lossy-UTF-8 string.
    value: KeyValue,
    /// Whether the collection value was truncated to [`MAX_INSPECT_ELEMENTS`].
    truncated: bool,
}

/// The value shape for [`KeyDetail`], discriminated by `kind` so the UI can render
/// a string vs a list of elements without guessing.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum KeyValue {
    /// A scalar string value (the `GET` result).
    String { data: String },
    /// A list/set/zset element list (each a lossy string; zset interleaves
    /// member then score).
    Elements { items: Vec<String> },
    /// A hash rendered as `[field, value, field, value, ...]`.
    Pairs { items: Vec<String> },
    /// The key does not exist.
    None,
}

/// One active pub/sub channel and its subscriber count.
#[derive(Debug, Clone, Serialize)]
struct ChannelRow {
    channel: String,
    subs: i64,
}

/// `GET /api/pubsub/channels` body.
#[derive(Debug, Clone, Serialize)]
struct ChannelsResponse {
    channels: Vec<ChannelRow>,
}

/// `GET /api/acl` body: the WHOAMI identity, the parsed user lines, and the
/// category list.
#[derive(Debug, Clone, Serialize)]
struct AclResponse {
    whoami: String,
    users: Vec<String>,
    categories: Vec<String>,
}

/// `GET /api/persistence` body: the persistence facts read from `INFO Persistence`.
#[derive(Debug, Clone, Serialize)]
struct PersistenceResponse {
    /// `rdb_last_save_time` (unix seconds), or `None` if absent.
    last_save_unixtime: Option<u64>,
    /// `rdb_changes_since_last_save`, or `None`.
    changes_since_save: Option<u64>,
    /// Whether an RDB save is configured (a `save` policy string is present and
    /// non-empty), best-effort from `rdb_bgsave_in_progress` / the raw map.
    rdb_enabled: bool,
    /// `aof_enabled` (`1` -> true), best-effort.
    aof_enabled: bool,
    /// `rdb_last_bgsave_status` (e.g. `ok` / `err`), if present.
    last_bgsave_status: Option<String>,
}

/// A JSON-friendly rendering of one [`RespValue`] for the arbitrary-command
/// console. Discriminated by `kind` so the UI can render each shape distinctly;
/// bytes are decoded lossily to a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum RenderedReply {
    /// A `+simple` string.
    Simple { value: String },
    /// A `-error` reply (the node's error text).
    Error { value: String },
    /// A `:integer`.
    Integer { value: i64 },
    /// A `$bulk` string (`null` -> `is_null: true`).
    Bulk { value: Option<String> },
    /// A `*array` of nested rendered replies.
    Array { items: Vec<RenderedReply> },
}

/// `POST /api/command` body: the rendered reply plus the command name (for audit
/// correlation; the UI echoes it).
#[derive(Debug, Clone, Serialize)]
struct CommandResponse {
    ok: bool,
    command: String,
    reply: RenderedReply,
}

/// One node's row in a `GET /api/cluster/rebalance-plan` response (#361, the dry-run
/// rail over engine #371/#444): its current vs balanced-target slot count and the
/// signed move (negative sheds, positive receives). Mirrors the engine `CLUSTER
/// REBALANCE DRYRUN` per-node map.
#[derive(Debug, Clone, Serialize)]
struct RebalanceTargetView {
    node: String,
    current_slots: i64,
    target_slots: i64,
    slots_to_move: i64,
}

/// `GET /api/cluster/rebalance-plan` response: the per-node plan the operator inspects
/// BEFORE any apply, plus a rollup. `total_slots_to_move` is the number of slots that
/// would change owner (the sum of the positive deltas, which equals the absolute sum
/// of the negative ones because rebalancing conserves slots); `balanced` is true when
/// the cluster already needs no moves. `dry_run` is always true: this endpoint never
/// mutates (the engine refuses APPLY).
#[derive(Debug, Clone, Serialize)]
struct RebalancePlanResponse {
    ok: bool,
    dry_run: bool,
    balanced: bool,
    total_slots_to_move: i64,
    targets: Vec<RebalanceTargetView>,
}

// ---- validation -------------------------------------------------------------

/// Whether `s` carries a CR or LF (a RESP-inline-injection / log-injection
/// hazard): such a token must be rejected before it is sent as a command arg.
fn has_crlf(s: &str) -> bool {
    s.contains('\r') || s.contains('\n')
}

/// Validate a CONFIG parameter token: non-empty, and no whitespace / CR / LF (a
/// config parameter name is always a single bare token).
fn validate_config_param(param: &str) -> Result<(), ValidationError> {
    if param.trim().is_empty() {
        return Err(ValidationError::Empty("config param must not be empty"));
    }
    if param.chars().any(char::is_whitespace) {
        return Err(ValidationError::Forbidden(
            "config param must not contain whitespace",
        ));
    }
    Ok(())
}

/// Validate a key name passed in the URL path: non-empty and no CR/LF (it is sent
/// as a single RESP bulk arg, so embedded CRLF cannot break framing, but a
/// CRLF-bearing key is rejected defensively and to keep audit lines single-line).
fn validate_key(key: &str) -> Result<(), ValidationError> {
    if key.is_empty() {
        return Err(ValidationError::Empty("key must not be empty"));
    }
    if has_crlf(key) {
        return Err(ValidationError::Forbidden("key must not contain CR or LF"));
    }
    Ok(())
}

/// Validate an ACL username: non-empty, no whitespace / CR / LF.
fn validate_username(username: &str) -> Result<(), ValidationError> {
    if username.trim().is_empty() {
        return Err(ValidationError::Empty("username must not be empty"));
    }
    if username.chars().any(char::is_whitespace) {
        return Err(ValidationError::Forbidden(
            "username must not contain whitespace",
        ));
    }
    Ok(())
}

/// Validate the arbitrary-command argv against the bounds.
fn validate_command_args(args: &[String]) -> Result<(), ValidationError> {
    if args.is_empty() {
        return Err(ValidationError::Empty("command args must not be empty"));
    }
    if args.len() > MAX_COMMAND_ARGS {
        return Err(ValidationError::TooLong("too many command args"));
    }
    let mut total = 0usize;
    for a in args {
        if a.len() > MAX_COMMAND_ARG_LEN {
            return Err(ValidationError::TooLong("a command arg is too long"));
        }
        total = total.saturating_add(a.len());
    }
    if total > MAX_COMMAND_TOTAL_LEN {
        return Err(ValidationError::TooLong("the command is too long in total"));
    }
    Ok(())
}

// ---- RESP -> JSON rendering --------------------------------------------------

/// Render a [`RespValue`] into a JSON-friendly [`RenderedReply`]. Pure and total:
/// every shape maps, bytes decode lossily, recursion mirrors the (already
/// depth-bounded) parser, so no extra bound is needed here.
#[must_use]
pub fn render_reply(value: &RespValue) -> RenderedReply {
    match value {
        RespValue::Simple(b) => RenderedReply::Simple {
            value: String::from_utf8_lossy(b).into_owned(),
        },
        RespValue::Error(b) => RenderedReply::Error {
            value: String::from_utf8_lossy(b).into_owned(),
        },
        RespValue::Integer(n) => RenderedReply::Integer { value: *n },
        RespValue::Bulk(None) => RenderedReply::Bulk { value: None },
        RespValue::Bulk(Some(b)) => RenderedReply::Bulk {
            value: Some(String::from_utf8_lossy(b).into_owned()),
        },
        RespValue::Array(items) => RenderedReply::Array {
            items: items.iter().map(render_reply).collect(),
        },
    }
}

/// Decode a RESP value's text body (simple / bulk) to a lossy string, or `None`.
fn text_of(v: &RespValue) -> Option<String> {
    v.as_text_bytes()
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

/// Read a RESP integer (`:n`, or a numeric bulk/simple) into an `i64`, or `None`.
fn int_of(v: &RespValue) -> Option<i64> {
    match v {
        RespValue::Integer(n) => Some(*n),
        other => other
            .as_text_bytes()
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.trim().parse().ok()),
    }
}

// ---- handlers (each: build RESP args -> command() -> JSON) -------------------
//
// Each handler takes an already-connected `NodeClient` and the parsed request,
// issues the command(s), and returns an `ApiResponse`. The HTTP layer opens the
// connection (so the topology lock is never held across node I/O), runs the tier
// gate first, and audit-logs the mutation outcome.

/// Map a [`NodeError`] from a node command into an [`ApiResponse`]. A node-level
/// command error (`-ERR ...`, an ACL `NOPERM`) is the NODE rejecting the request,
/// so it is surfaced as a `200`-shaped JSON error from the node where helpful, but
/// for the management endpoints we report it as a `502` (the upstream node) with
/// the node's text, EXCEPT a `Command` error which we pass through as the node's
/// own message so the UI shows e.g. the ACL denial verbatim.
fn node_error_response(e: &NodeError) -> ApiResponse {
    match e {
        // The node answered with a `-ERR` (bad arg, ACL denial): the request
        // reached the node and was rejected; show the node's text. 502 = upstream.
        NodeError::Command(msg) | NodeError::Auth(msg) => {
            ApiResponse::error(502, &format!("node rejected the command: {msg}"))
        }
        // Transport / timeout / protocol: the node is unreachable or misbehaving.
        other => ApiResponse::error(502, &format!("node error: {other}")),
    }
}

/// `GET /api/config`: `CONFIG GET *` -> sorted `[{param, value}]`.
///
/// # Errors
///
/// Returns a `502` [`ApiResponse`] when the node command fails.
pub async fn config_get(client: &mut NodeClient) -> ApiResponse {
    let reply = match client.command(&[b"CONFIG", b"GET", b"*"]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    let RespValue::Array(items) = reply else {
        return ApiResponse::error(502, "unexpected CONFIG GET reply (not an array)");
    };
    // CONFIG GET returns a flat [param, value, param, value, ...] array.
    let mut params = Vec::with_capacity(items.len() / 2);
    let mut it = items.chunks_exact(2);
    for pair in it.by_ref() {
        let (Some(param), Some(value)) = (text_of(&pair[0]), text_of(&pair[1])) else {
            continue;
        };
        params.push(ConfigRow { param, value });
    }
    params.sort_by(|a, b| a.param.cmp(&b.param));
    ApiResponse::ok(&ConfigResponse { params })
}

/// `POST /api/config`: `CONFIG SET <param> <value>` -> `{ok}`.
///
/// # Errors
///
/// Returns a `400` for an invalid param, or a `502` when the node rejects the set.
pub async fn config_set(client: &mut NodeClient, body: &ConfigSetBody) -> ApiResponse {
    if let Err(e) = validate_config_param(&body.param) {
        return ApiResponse::bad_request(&e.to_string());
    }
    let reply = match client
        .command(&[
            b"CONFIG",
            b"SET",
            body.param.as_bytes(),
            body.value.as_bytes(),
        ])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// `GET /api/cluster/rebalance-plan` (#361, the rebalance dry-run rail over engine
/// #444): issue `CLUSTER REBALANCE DRYRUN` and render the per-node slot-balance plan
/// so an operator sees the slot diff BEFORE any apply. READ-ONLY: the engine mutates
/// nothing and refuses APPLY, and this is a GET. Admin-tier (the HTTP gate enforces
/// the privileged role); routed through the configured node, which re-checks the
/// committed epoch in raft mode.
///
/// # Errors
///
/// Returns a `502` when the node rejects the command or replies with an unexpected
/// shape (e.g. cluster support disabled, or a non-plan reply).
pub async fn cluster_rebalance_plan(client: &mut NodeClient) -> ApiResponse {
    let reply = match client.command(&[b"CLUSTER", b"REBALANCE", b"DRYRUN"]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    match parse_rebalance_plan(&reply) {
        Some(plan) => ApiResponse::ok(&plan),
        None => ApiResponse::error(
            502,
            "unexpected CLUSTER REBALANCE reply (not a per-node plan array)",
        ),
    }
}

/// The literal token a [`FailoverBody`] must carry to authorize a failover.
pub const FAILOVER_CONFIRM: &str = "FAILOVER";

/// `POST /api/cluster/failover` (#361, the first MUTATING cluster action): issue a BARE
/// `CLUSTER FAILOVER` to the configured node. In raft mode the engine proposes a
/// committed `PromoteReplica` of this node's slots through the leader (#443).
///
/// SAFE BY ENGINE CONSTRUCTION: the engine refuses the failover unless THIS node is an
/// in-sync replica (the exact promotion gate the automatic path uses, ADR-0026) and
/// rejects `FORCE`/`TAKEOVER`, so the console cannot bypass the data-safety gate; a
/// rejected failover comes back as the node's verbatim `-ERR` (a `502`).
///
/// DESTRUCTIVE-CONFIRMATION: the body must carry `{"confirm":"FAILOVER"}` ([`FAILOVER_CONFIRM`])
/// or it is a `400`, so a stray / replayed POST cannot trigger a promotion. The route is
/// Admin-tier (mutation) and audit-logged by the HTTP layer.
///
/// # Errors
///
/// Returns a `400` when the confirmation token is absent/wrong, or a `502` when the node
/// rejects the failover (e.g. this node is not an in-sync replica) or is unreachable.
pub async fn cluster_failover(client: &mut NodeClient, body: &FailoverBody) -> ApiResponse {
    if !failover_confirmed(body) {
        return ApiResponse::bad_request(
            "destructive action: re-send with {\"confirm\":\"FAILOVER\"} to trigger a failover",
        );
    }
    let reply = match client.command(&[b"CLUSTER", b"FAILOVER"]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// Whether a [`FailoverBody`] carries the EXACT confirmation token (trailing form
/// whitespace tolerated, but the match is otherwise exact + case-sensitive so the
/// operator must type the deliberate token, not a near-miss). Pure, so the
/// destructive-confirmation rail is unit-tested without a node.
fn failover_confirmed(body: &FailoverBody) -> bool {
    body.confirm.trim() == FAILOVER_CONFIRM
}

/// `POST /api/cluster/meet` (#361): add a node via `CLUSTER MEET host port`. In raft
/// mode the engine forwards the committed membership add through the leader. Additive,
/// so no destructive-confirmation; the engine is the authority on address validity.
/// Admin-tier + audit-logged by the HTTP layer.
///
/// # Errors
///
/// Returns a `400` for an empty / CRLF-bearing host or a `0` port, or a `502` when the
/// node rejects the address or is unreachable.
pub async fn cluster_meet(client: &mut NodeClient, body: &MeetBody) -> ApiResponse {
    let host = body.host.trim();
    if host.is_empty() || has_crlf(&body.host) {
        return ApiResponse::bad_request("host must be a non-empty token without CR or LF");
    }
    if body.port == 0 {
        return ApiResponse::bad_request("port must be in 1..=65535");
    }
    let port = body.port.to_string();
    // Send the TRIMMED host (we validated on the trim), so a stray space does not reach
    // the engine as part of the address and earn a confusing rejection.
    let reply = match client
        .command(&[b"CLUSTER", b"MEET", host.as_bytes(), port.as_bytes()])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// `POST /api/cluster/forget` (#361): remove a node via `CLUSTER FORGET node-id`.
/// DESTRUCTIVE: `confirm` must ECHO the `node_id` (so a stray POST cannot forget the
/// wrong node); the engine still rejects forgetting self / an unknown id. Admin-tier +
/// audit-logged.
///
/// # Errors
///
/// Returns a `400` for an empty / CRLF-bearing id or a confirmation that does not match
/// the id, or a `502` when the node rejects the forget or is unreachable.
pub async fn cluster_forget(client: &mut NodeClient, body: &ForgetBody) -> ApiResponse {
    let node_id = body.node_id.trim();
    if node_id.is_empty() || has_crlf(&body.node_id) {
        return ApiResponse::bad_request("node_id must be a non-empty token without CR or LF");
    }
    if !forget_confirmed(body) {
        return ApiResponse::bad_request(
            "destructive action: set confirm to the exact node_id being forgotten",
        );
    }
    let reply = match client
        .command(&[b"CLUSTER", b"FORGET", node_id.as_bytes()])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// Whether a [`ForgetBody`]'s `confirm` ECHOES its `node_id` (both trimmed) and the id
/// is non-empty: the "type the target to confirm" rail. Pure, so the forget guard is
/// unit-tested without a node.
fn forget_confirmed(body: &ForgetBody) -> bool {
    let id = body.node_id.trim();
    !id.is_empty() && body.confirm.trim() == id
}

/// Parse a RESP2 `CLUSTER REBALANCE DRYRUN` reply into a [`RebalancePlanResponse`].
/// The reply is an array of per-node maps; over the console's RESP2 link each map is a
/// FLAT `[key, value, ...]` array (the engine degrades RESP3 maps to a flat RESP2
/// array, [`ironcache_protocol`] encode.rs). PURE + total: a non-array reply, or a row
/// missing a field, yields `None` (the caller maps that to a `502`). Unit-tested
/// without a node.
fn parse_rebalance_plan(reply: &RespValue) -> Option<RebalancePlanResponse> {
    let RespValue::Array(rows) = reply else {
        return None;
    };
    let mut targets = Vec::with_capacity(rows.len());
    let mut total_slots_to_move = 0i64;
    for row in rows {
        let target = parse_rebalance_row(row)?;
        // Conservation: the positive deltas equal the absolute negative deltas, so
        // summing the positives counts each relocating slot exactly once.
        if target.slots_to_move > 0 {
            total_slots_to_move = total_slots_to_move.saturating_add(target.slots_to_move);
        }
        targets.push(target);
    }
    Some(RebalancePlanResponse {
        ok: true,
        dry_run: true,
        balanced: total_slots_to_move == 0,
        total_slots_to_move,
        targets,
    })
}

/// Parse ONE node row (a RESP2 flat `[key, value, ...]` map) into a
/// [`RebalanceTargetView`]. Field order is NOT assumed: each value is looked up by its
/// key, so a future engine reorder cannot misread it. Returns `None` if any of the
/// four fields is absent or the wrong type.
fn parse_rebalance_row(row: &RespValue) -> Option<RebalanceTargetView> {
    let RespValue::Array(kvs) = row else {
        return None;
    };
    let mut node = None;
    let mut current_slots = None;
    let mut target_slots = None;
    let mut slots_to_move = None;
    for pair in kvs.chunks_exact(2) {
        let Some(key) = text_of(&pair[0]) else {
            continue;
        };
        match key.as_str() {
            "node" => node = text_of(&pair[1]),
            "current_slots" => current_slots = int_of(&pair[1]),
            "target_slots" => target_slots = int_of(&pair[1]),
            "slots_to_move" => slots_to_move = int_of(&pair[1]),
            _ => {}
        }
    }
    Some(RebalanceTargetView {
        node: node?,
        current_slots: current_slots?,
        target_slots: target_slots?,
        slots_to_move: slots_to_move?,
    })
}

/// `GET /api/keys?pattern=&cursor=&count=`: `SCAN cursor MATCH pattern COUNT count`,
/// then a bounded `TYPE` + `TTL` per returned key (capped at [`MAX_SCAN_KEYS`]).
///
/// # Errors
///
/// Returns a `400` for an over-long pattern, or a `502` when the node errors.
pub async fn keys_scan(
    client: &mut NodeClient,
    cursor: &str,
    pattern: &str,
    count: u64,
) -> ApiResponse {
    if pattern.len() > MAX_PATTERN_LEN {
        return ApiResponse::bad_request("SCAN pattern is too long");
    }
    if has_crlf(pattern) || has_crlf(cursor) {
        return ApiResponse::bad_request("SCAN pattern/cursor must not contain CR or LF");
    }
    // The cursor is a node-supplied opaque integer string; default to "0".
    let cursor = if cursor.trim().is_empty() {
        "0"
    } else {
        cursor
    };
    let count = count.clamp(1, MAX_SCAN_COUNT);
    let pattern = if pattern.is_empty() { "*" } else { pattern };
    let count_s = count.to_string();
    let reply = match client
        .command(&[
            b"SCAN",
            cursor.as_bytes(),
            b"MATCH",
            pattern.as_bytes(),
            b"COUNT",
            count_s.as_bytes(),
        ])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    // SCAN replies `[next_cursor, [key, key, ...]]`.
    let RespValue::Array(top) = reply else {
        return ApiResponse::error(502, "unexpected SCAN reply (not an array)");
    };
    let next_cursor = top
        .first()
        .and_then(text_of)
        .unwrap_or_else(|| "0".to_owned());
    let key_names: Vec<String> = match top.get(1) {
        Some(RespValue::Array(items)) => items
            .iter()
            .filter_map(text_of)
            .take(MAX_SCAN_KEYS)
            .collect(),
        _ => Vec::new(),
    };
    let mut keys = Vec::with_capacity(key_names.len());
    for name in key_names {
        // TYPE + TTL per key, each bounded by the node op timeout. A per-key error
        // degrades that key's metadata rather than failing the whole page.
        let ktype = match client.command(&[b"TYPE", name.as_bytes()]).await {
            Ok(r) => text_of(&r).unwrap_or_else(|| "unknown".to_owned()),
            Err(e) => return node_error_response(&e),
        };
        let ttl = match client.command(&[b"TTL", name.as_bytes()]).await {
            Ok(r) => int_of(&r).unwrap_or(-2),
            Err(e) => return node_error_response(&e),
        };
        keys.push(ScanKey {
            key: name,
            r#type: ktype,
            ttl,
        });
    }
    ApiResponse::ok(&ScanResponse {
        cursor: next_cursor,
        keys,
    })
}

/// `GET /api/keys/{k}`: `TYPE` + `TTL` + value (per type, bounded). 404 if missing.
///
/// # Errors
///
/// Returns a `400` for a bad key, `404` if the key is gone, or `502` on a node
/// error.
pub async fn key_get(client: &mut NodeClient, key: &str) -> ApiResponse {
    if let Err(e) = validate_key(key) {
        return ApiResponse::bad_request(&e.to_string());
    }
    let kb = key.as_bytes();
    let ktype = match client.command(&[b"TYPE", kb]).await {
        Ok(r) => text_of(&r).unwrap_or_else(|| "none".to_owned()),
        Err(e) => return node_error_response(&e),
    };
    if ktype == "none" {
        return ApiResponse::not_found(&format!("no key {key}"));
    }
    let ttl = match client.command(&[b"TTL", kb]).await {
        Ok(r) => int_of(&r).unwrap_or(-1),
        Err(e) => return node_error_response(&e),
    };
    let cap = MAX_INSPECT_ELEMENTS.to_string();
    let (value, truncated) = match ktype.as_str() {
        "string" => match client.command(&[b"GET", kb]).await {
            Ok(r) => (
                KeyValue::String {
                    data: text_of(&r).unwrap_or_default(),
                },
                false,
            ),
            Err(e) => return node_error_response(&e),
        },
        "list" => match client.command(&[b"LRANGE", kb, b"0", cap.as_bytes()]).await {
            Ok(RespValue::Array(items)) => {
                let trunc = items.len() as i64 > MAX_INSPECT_ELEMENTS;
                (
                    KeyValue::Elements {
                        items: items.iter().filter_map(text_of).collect(),
                    },
                    trunc,
                )
            }
            Ok(_) => (KeyValue::Elements { items: Vec::new() }, false),
            Err(e) => return node_error_response(&e),
        },
        "set" => match client
            .command(&[b"SSCAN", kb, b"0", b"COUNT", cap.as_bytes()])
            .await
        {
            // SSCAN returns [cursor, [members...]]; render the members page.
            Ok(RespValue::Array(top)) => {
                let items: Vec<String> = match top.get(1) {
                    Some(RespValue::Array(m)) => m.iter().filter_map(text_of).collect(),
                    _ => Vec::new(),
                };
                let trunc = top.first().and_then(text_of).as_deref() != Some("0");
                (KeyValue::Elements { items }, trunc)
            }
            Ok(_) => (KeyValue::Elements { items: Vec::new() }, false),
            Err(e) => return node_error_response(&e),
        },
        "hash" => match client.command(&[b"HGETALL", kb]).await {
            Ok(RespValue::Array(items)) => {
                let trunc = items.len() as i64 > MAX_INSPECT_ELEMENTS * 2;
                let taken: Vec<String> = items
                    .iter()
                    .take((MAX_INSPECT_ELEMENTS * 2) as usize)
                    .filter_map(text_of)
                    .collect();
                (KeyValue::Pairs { items: taken }, trunc)
            }
            Ok(_) => (KeyValue::Pairs { items: Vec::new() }, false),
            Err(e) => return node_error_response(&e),
        },
        "zset" => {
            match client
                .command(&[b"ZRANGE", kb, b"0", cap.as_bytes(), b"WITHSCORES"])
                .await
            {
                Ok(RespValue::Array(items)) => {
                    let trunc = items.len() as i64 > MAX_INSPECT_ELEMENTS * 2;
                    (
                        KeyValue::Elements {
                            items: items.iter().filter_map(text_of).collect(),
                        },
                        trunc,
                    )
                }
                Ok(_) => (KeyValue::Elements { items: Vec::new() }, false),
                Err(e) => return node_error_response(&e),
            }
        }
        // An unmodeled type (stream etc.): report the type, leave the value None.
        _ => (KeyValue::None, false),
    };
    ApiResponse::ok(&KeyDetail {
        key: key.to_owned(),
        r#type: ktype,
        ttl,
        value,
        truncated,
    })
}

/// `POST /api/keys/{k}`: `SET k value` -> `{ok}`.
///
/// # Errors
///
/// Returns a `400` for a bad key / over-long value, or `502` on a node error.
pub async fn key_set(client: &mut NodeClient, key: &str, body: &KeySetBody) -> ApiResponse {
    if let Err(e) = validate_key(key) {
        return ApiResponse::bad_request(&e.to_string());
    }
    if body.value.len() > MAX_VALUE_LEN {
        return ApiResponse::bad_request("value is too long");
    }
    let reply = match client
        .command(&[b"SET", key.as_bytes(), body.value.as_bytes()])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// `DELETE /api/keys/{k}`: `DEL k` -> `{deleted}`.
///
/// # Errors
///
/// Returns a `400` for a bad key, or `502` on a node error.
pub async fn key_delete(client: &mut NodeClient, key: &str) -> ApiResponse {
    if let Err(e) = validate_key(key) {
        return ApiResponse::bad_request(&e.to_string());
    }
    let reply = match client.command(&[b"DEL", key.as_bytes()]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ApiResponse::ok(&DeletedResponse {
        deleted: int_of(&reply).unwrap_or(0),
    })
}

/// `POST /api/keys/{k}/expire`: `EXPIRE k seconds` -> `{ok}` (false if 0 keys set).
///
/// # Errors
///
/// Returns a `400` for a bad key / negative seconds, or `502` on a node error.
pub async fn key_expire(client: &mut NodeClient, key: &str, body: &ExpireBody) -> ApiResponse {
    if let Err(e) = validate_key(key) {
        return ApiResponse::bad_request(&e.to_string());
    }
    if body.seconds < 0 {
        return ApiResponse::bad_request("seconds must be a non-negative integer");
    }
    let secs = body.seconds.to_string();
    let reply = match client
        .command(&[b"EXPIRE", key.as_bytes(), secs.as_bytes()])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    // EXPIRE returns 1 if the timeout was set, 0 if the key does not exist.
    ApiResponse::ok(&OkResponse {
        ok: int_of(&reply).unwrap_or(0) == 1,
    })
}

/// `POST /api/keys/{k}/persist`: `PERSIST k` -> `{ok}`.
///
/// # Errors
///
/// Returns a `400` for a bad key, or `502` on a node error.
pub async fn key_persist(client: &mut NodeClient, key: &str) -> ApiResponse {
    if let Err(e) = validate_key(key) {
        return ApiResponse::bad_request(&e.to_string());
    }
    let reply = match client.command(&[b"PERSIST", key.as_bytes()]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ApiResponse::ok(&OkResponse {
        ok: int_of(&reply).unwrap_or(0) == 1,
    })
}

/// `POST /api/command`: run an arbitrary command argv -> `{ok, command, reply}`.
/// The command name is audit-logged by the caller. There is NO client-side
/// allowlist: the node ACL bounds it (defense in depth). A node `-ERR` reply is
/// rendered into `reply.kind = "error"` (a `200` with the node's text), so the
/// console shows the node's response rather than masking it.
///
/// # Errors
///
/// Returns a `400` for an empty / over-bounds argv, or `502` on a transport error.
pub async fn run_command(client: &mut NodeClient, body: &CommandBody) -> ApiResponse {
    if let Err(e) = validate_command_args(&body.args) {
        return ApiResponse::bad_request(&e.to_string());
    }
    for a in &body.args {
        if has_crlf(a) {
            return ApiResponse::bad_request("command arg must not contain CR or LF");
        }
    }
    let command_name = body.args.first().cloned().unwrap_or_default();
    // AUDIT (#361): log the command NAME (truncated) and arg COUNT only, never the
    // args/values (an arg may be a value being SET, or a secret). The name is
    // attacker-controlled and bounded only by the per-arg cap, so cap the audit
    // copy to keep a single audit line small. The generic mutation audit in the
    // HTTP layer logs the path + status; this adds the command name.
    tracing::info!(
        command = %audit_name(&command_name),
        arg_count = body.args.len(),
        "console command runner: executing"
    );
    let arg_refs: Vec<&[u8]> = body.args.iter().map(String::as_bytes).collect();
    // A node `-ERR` reply must be SHOWN (not turned into a 502), so call the inner
    // command and special-case the Command error into a rendered error reply.
    match client.command(&arg_refs).await {
        Ok(reply) => ApiResponse::ok(&CommandResponse {
            ok: true,
            command: command_name,
            reply: render_reply(&reply),
        }),
        Err(NodeError::Command(msg)) => ApiResponse::ok(&CommandResponse {
            ok: false,
            command: command_name,
            reply: RenderedReply::Error { value: msg },
        }),
        Err(e) => node_error_response(&e),
    }
}

/// `GET /api/pubsub/channels`: `PUBSUB CHANNELS` then `PUBSUB NUMSUB <chans...>`
/// -> `[{channel, subs}]`.
///
/// # Errors
///
/// Returns a `502` on a node error.
pub async fn pubsub_channels(client: &mut NodeClient) -> ApiResponse {
    let reply = match client.command(&[b"PUBSUB", b"CHANNELS"]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    let RespValue::Array(items) = reply else {
        return ApiResponse::error(502, "unexpected PUBSUB CHANNELS reply (not an array)");
    };
    let names: Vec<String> = items.iter().filter_map(text_of).collect();
    if names.is_empty() {
        return ApiResponse::ok(&ChannelsResponse {
            channels: Vec::new(),
        });
    }
    // PUBSUB NUMSUB ch1 ch2 ... -> [ch1, count1, ch2, count2, ...].
    let mut args: Vec<&[u8]> = Vec::with_capacity(names.len() + 2);
    args.push(b"PUBSUB");
    args.push(b"NUMSUB");
    for n in &names {
        args.push(n.as_bytes());
    }
    let numsub = match client.command(&args).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    let mut channels = Vec::with_capacity(names.len());
    if let RespValue::Array(pairs) = numsub {
        for pair in pairs.chunks_exact(2) {
            if let Some(channel) = text_of(&pair[0]) {
                channels.push(ChannelRow {
                    channel,
                    subs: int_of(&pair[1]).unwrap_or(0),
                });
            }
        }
    }
    // If NUMSUB came back empty/odd, fall back to the channel names with 0.
    if channels.is_empty() {
        channels = names
            .into_iter()
            .map(|channel| ChannelRow { channel, subs: 0 })
            .collect();
    }
    ApiResponse::ok(&ChannelsResponse { channels })
}

/// `POST /api/pubsub/publish`: `PUBLISH channel message` -> `{receivers}`.
///
/// # Errors
///
/// Returns a `400` for an empty channel / over-long message, or `502` on a node
/// error.
pub async fn pubsub_publish(client: &mut NodeClient, body: &PublishBody) -> ApiResponse {
    if body.channel.trim().is_empty() {
        return ApiResponse::bad_request("channel must not be empty");
    }
    if has_crlf(&body.channel) {
        return ApiResponse::bad_request("channel must not contain CR or LF");
    }
    if body.message.len() > MAX_MESSAGE_LEN {
        return ApiResponse::bad_request("message is too long");
    }
    let reply = match client
        .command(&[b"PUBLISH", body.channel.as_bytes(), body.message.as_bytes()])
        .await
    {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ApiResponse::ok(&PublishResponse {
        receivers: int_of(&reply).unwrap_or(0),
    })
}

/// `GET /api/acl`: `ACL WHOAMI`, `ACL LIST`, `ACL CAT` -> `{whoami, users, categories}`.
///
/// # Errors
///
/// Returns a `502` on a node error.
pub async fn acl_get(client: &mut NodeClient) -> ApiResponse {
    let whoami = match client.command(&[b"ACL", b"WHOAMI"]).await {
        Ok(r) => text_of(&r).unwrap_or_default(),
        Err(e) => return node_error_response(&e),
    };
    let users = match client.command(&[b"ACL", b"LIST"]).await {
        Ok(RespValue::Array(items)) => items.iter().filter_map(text_of).collect(),
        Ok(_) => Vec::new(),
        Err(e) => return node_error_response(&e),
    };
    let categories = match client.command(&[b"ACL", b"CAT"]).await {
        Ok(RespValue::Array(items)) => items.iter().filter_map(text_of).collect(),
        Ok(_) => Vec::new(),
        Err(e) => return node_error_response(&e),
    };
    ApiResponse::ok(&AclResponse {
        whoami,
        users,
        categories,
    })
}

/// Truncate an attacker-controlled command name for a single-line audit log entry.
/// The full name is bounded only by the per-arg cap (64 KiB), so cap the audited
/// copy to keep the log line small (the response still carries the real name).
fn audit_name(name: &str) -> String {
    const MAX: usize = 48;
    if name.len() <= MAX {
        return name.to_owned();
    }
    let mut end = MAX;
    while end > 0 && !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &name[..end])
}

/// The username the console itself is authenticated as on the node (`ACL WHOAMI`),
/// or `None` if it cannot be determined. Used to refuse an ACL mutation that would
/// modify the console's OWN user and risk locking the console out of the node.
async fn console_own_user(client: &mut NodeClient) -> Option<String> {
    match client.command(&[b"ACL", b"WHOAMI"]).await {
        Ok(r) => text_of(&r),
        Err(_) => None,
    }
}

/// Whether `target` names the console's own connected user (case-insensitive on
/// the ASCII username). Best-effort: if WHOAMI is unavailable, returns false (the
/// node ACL remains the ultimate bound).
async fn targets_console_own_user(client: &mut NodeClient, target: &str) -> bool {
    console_own_user(client)
        .await
        .is_some_and(|u| u.eq_ignore_ascii_case(target))
}

/// `POST /api/acl/user`: `ACL SETUSER username rules...` -> `{ok}`.
///
/// # Errors
///
/// Returns a `400` for a bad username / rule token, a `409` when it targets the
/// console's own user (self-lockout guard), or `502` on a node error.
pub async fn acl_setuser(client: &mut NodeClient, body: &AclUserBody) -> ApiResponse {
    if let Err(e) = validate_username(&body.username) {
        return ApiResponse::bad_request(&e.to_string());
    }
    if targets_console_own_user(client, &body.username).await {
        return ApiResponse::error(
            409,
            "refusing to modify the console's own ACL user (would risk locking the \
             console out of the node); change that user server-side instead",
        );
    }
    if body.rules.len() > MAX_ACL_RULES {
        return ApiResponse::bad_request("too many ACL rules");
    }
    for r in &body.rules {
        if has_crlf(r) {
            return ApiResponse::bad_request("an ACL rule must not contain CR or LF");
        }
    }
    let mut args: Vec<&[u8]> = Vec::with_capacity(body.rules.len() + 3);
    args.push(b"ACL");
    args.push(b"SETUSER");
    args.push(body.username.as_bytes());
    for r in &body.rules {
        args.push(r.as_bytes());
    }
    let reply = match client.command(&args).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// `DELETE /api/acl/user/{name}`: `ACL DELUSER name` -> `{deleted}`.
///
/// # Errors
///
/// Returns a `400` for a bad name, a `409` when it targets the console's own user
/// (self-lockout guard), or `502` on a node error.
pub async fn acl_deluser(client: &mut NodeClient, name: &str) -> ApiResponse {
    if let Err(e) = validate_username(name) {
        return ApiResponse::bad_request(&e.to_string());
    }
    if targets_console_own_user(client, name).await {
        return ApiResponse::error(
            409,
            "refusing to delete the console's own ACL user (would lock the console \
             out of the node); delete it server-side instead",
        );
    }
    let reply = match client.command(&[b"ACL", b"DELUSER", name.as_bytes()]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ApiResponse::ok(&DeletedResponse {
        deleted: int_of(&reply).unwrap_or(0),
    })
}

/// `GET /api/persistence`: from `INFO persistence` -> the persistence facts.
///
/// # Errors
///
/// Returns a `502` on a node error.
pub async fn persistence_get(client: &mut NodeClient) -> ApiResponse {
    let body = match client.command(&[b"INFO", b"persistence"]).await {
        Ok(r) => match r.as_text_bytes() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return ApiResponse::error(502, "unexpected INFO reply (not text)"),
        },
        Err(e) => return node_error_response(&e),
    };
    let info = crate::info::parse_info(&body);
    let raw_u64 = |k: &str| info.raw.get(k).and_then(|v| v.trim().parse::<u64>().ok());
    // RDB is "enabled" when a non-empty save policy is reported (rdb_last_save_time
    // present is also a strong hint). AOF from `aof_enabled:1`.
    let rdb_enabled =
        info.rdb_last_save_time.is_some() || info.raw.contains_key("rdb_bgsave_in_progress");
    let aof_enabled = info.raw.get("aof_enabled").map(String::as_str) == Some("1");
    ApiResponse::ok(&PersistenceResponse {
        last_save_unixtime: info.rdb_last_save_time,
        changes_since_save: info.rdb_changes_since_last_save,
        rdb_enabled,
        aof_enabled,
        last_bgsave_status: info
            .raw
            .get("rdb_last_bgsave_status")
            .cloned()
            .or_else(|| raw_u64("rdb_last_bgsave_status").map(|n| n.to_string())),
    })
}

/// `POST /api/persistence/save`: `BGSAVE` (default) or `SAVE` -> `{ok}`.
///
/// # Errors
///
/// Returns a `502` on a node error.
pub async fn persistence_save(client: &mut NodeClient, body: &SaveBody) -> ApiResponse {
    let cmd: &[u8] = if body.background { b"BGSAVE" } else { b"SAVE" };
    let reply = match client.command(&[cmd]).await {
        Ok(r) => r,
        Err(e) => return node_error_response(&e),
    };
    ok_or_node_text(&reply)
}

/// Map a reply that should be `+OK` (or a `+Background saving started`) into
/// `{ok:true}`, else surface the node's text as a `502`. A simple/bulk text reply
/// is treated as success (the node acknowledged); anything else is an error.
fn ok_or_node_text(reply: &RespValue) -> ApiResponse {
    match reply {
        RespValue::Simple(_) | RespValue::Bulk(Some(_)) => {
            ApiResponse::ok(&OkResponse { ok: true })
        }
        RespValue::Integer(_) => ApiResponse::ok(&OkResponse { ok: true }),
        RespValue::Error(b) => ApiResponse::error(
            502,
            &format!("node rejected the command: {}", String::from_utf8_lossy(b)),
        ),
        other => ApiResponse::error(502, &format!("unexpected node reply: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_config_param_rejects_empty_and_whitespace() {
        assert!(validate_config_param("maxmemory").is_ok());
        assert!(matches!(
            validate_config_param(""),
            Err(ValidationError::Empty(_))
        ));
        assert!(matches!(
            validate_config_param("   "),
            Err(ValidationError::Empty(_))
        ));
        assert!(matches!(
            validate_config_param("max memory"),
            Err(ValidationError::Forbidden(_))
        ));
        assert!(matches!(
            validate_config_param("max\r\nmemory"),
            Err(ValidationError::Forbidden(_))
        ));
    }

    #[test]
    fn audit_name_truncates_long_names_on_a_char_boundary() {
        // A short name is unchanged.
        assert_eq!(audit_name("CONFIG"), "CONFIG");
        // A long name is capped with an ellipsis (keeps a single audit line small).
        let long = "A".repeat(200);
        let out = audit_name(&long);
        assert!(out.len() <= 48 + 3, "{out}");
        assert!(out.ends_with("..."), "{out}");
        // A multibyte char straddling the cap does not panic and yields valid UTF-8.
        let multi = "x".repeat(47) + "\u{1F600}" + &"y".repeat(60);
        let out = audit_name(&multi);
        assert!(out.ends_with("..."));
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    #[test]
    fn validate_key_rejects_empty_and_crlf() {
        assert!(validate_key("user:1").is_ok());
        assert!(matches!(validate_key(""), Err(ValidationError::Empty(_))));
        assert!(matches!(
            validate_key("a\nb"),
            Err(ValidationError::Forbidden(_))
        ));
        assert!(matches!(
            validate_key("a\rb"),
            Err(ValidationError::Forbidden(_))
        ));
        // A key WITH a space is allowed (a value/key may contain spaces; it is one
        // RESP bulk arg). Only CR/LF are rejected.
        assert!(validate_key("a b").is_ok());
    }

    #[test]
    fn validate_username_rejects_empty_whitespace_crlf() {
        assert!(validate_username("alice").is_ok());
        assert!(matches!(
            validate_username(""),
            Err(ValidationError::Empty(_))
        ));
        assert!(matches!(
            validate_username("al ice"),
            Err(ValidationError::Forbidden(_))
        ));
        assert!(matches!(
            validate_username("al\nice"),
            Err(ValidationError::Forbidden(_))
        ));
    }

    #[test]
    fn validate_command_args_bounds() {
        assert!(validate_command_args(&["PING".to_owned()]).is_ok());
        assert!(matches!(
            validate_command_args(&[]),
            Err(ValidationError::Empty(_))
        ));
        // Too many args.
        let many: Vec<String> = (0..=MAX_COMMAND_ARGS).map(|i| i.to_string()).collect();
        assert!(matches!(
            validate_command_args(&many),
            Err(ValidationError::TooLong(_))
        ));
        // One over-long arg.
        let big = vec!["GET".to_owned(), "x".repeat(MAX_COMMAND_ARG_LEN + 1)];
        assert!(matches!(
            validate_command_args(&big),
            Err(ValidationError::TooLong(_))
        ));
    }

    #[test]
    fn render_reply_maps_each_resp_shape() {
        assert_eq!(
            render_reply(&RespValue::Simple(b"OK".to_vec())),
            RenderedReply::Simple {
                value: "OK".to_owned()
            }
        );
        assert_eq!(
            render_reply(&RespValue::Error(b"ERR nope".to_vec())),
            RenderedReply::Error {
                value: "ERR nope".to_owned()
            }
        );
        assert_eq!(
            render_reply(&RespValue::Integer(7)),
            RenderedReply::Integer { value: 7 }
        );
        assert_eq!(
            render_reply(&RespValue::Bulk(None)),
            RenderedReply::Bulk { value: None }
        );
        assert_eq!(
            render_reply(&RespValue::Bulk(Some(b"hi".to_vec()))),
            RenderedReply::Bulk {
                value: Some("hi".to_owned())
            }
        );
        let nested = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Bulk(Some(b"a".to_vec())),
        ]);
        assert_eq!(
            render_reply(&nested),
            RenderedReply::Array {
                items: vec![
                    RenderedReply::Integer { value: 1 },
                    RenderedReply::Bulk {
                        value: Some("a".to_owned())
                    },
                ]
            }
        );
    }

    #[test]
    fn render_reply_serializes_with_kind_tag() {
        let v = render_reply(&RespValue::Integer(42));
        let json = serde_json::to_string(&v).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["kind"], "integer");
        assert_eq!(parsed["value"], 42);
        // A nested array carries kind=array with items.
        let arr = render_reply(&RespValue::Array(vec![RespValue::Simple(b"x".to_vec())]));
        let json = serde_json::to_string(&arr).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["kind"], "array");
        assert_eq!(parsed["items"][0]["kind"], "simple");
        assert_eq!(parsed["items"][0]["value"], "x");
    }

    #[test]
    fn int_of_and_text_of_extract_expected() {
        assert_eq!(int_of(&RespValue::Integer(5)), Some(5));
        assert_eq!(int_of(&RespValue::Bulk(Some(b"12".to_vec()))), Some(12));
        assert_eq!(int_of(&RespValue::Bulk(None)), None);
        assert_eq!(
            text_of(&RespValue::Bulk(Some(b"hello".to_vec()))).as_deref(),
            Some("hello")
        );
        assert_eq!(text_of(&RespValue::Integer(1)), None);
    }

    // ---- CLUSTER FAILOVER destructive-confirmation (#361) ----

    #[test]
    fn failover_requires_the_exact_confirmation_token() {
        let body = |c: &str| FailoverBody {
            confirm: c.to_owned(),
        };
        // The exact token (with tolerated surrounding whitespace) confirms.
        assert!(failover_confirmed(&body("FAILOVER")));
        assert!(failover_confirmed(&body("  FAILOVER  ")));
        // A near-miss does NOT: wrong case, empty, partial, or an extra char. A stray /
        // replayed POST without the deliberate token can never trigger a promotion.
        assert!(!failover_confirmed(&body("failover")));
        assert!(!failover_confirmed(&body("")));
        assert!(!failover_confirmed(&body("FAIL")));
        assert!(!failover_confirmed(&body("FAILOVER!")));
        assert!(!failover_confirmed(&body("yes")));
    }

    #[test]
    fn forget_requires_confirm_to_echo_the_node_id() {
        let body = |id: &str, confirm: &str| ForgetBody {
            node_id: id.to_owned(),
            confirm: confirm.to_owned(),
        };
        let id = "1111111111111111111111111111111111111111";
        // confirm echoes the id (surrounding whitespace tolerated on both sides).
        assert!(forget_confirmed(&body(id, id)));
        assert!(forget_confirmed(&body(id, &format!("  {id}  "))));
        // A mismatched confirm (a DIFFERENT id, a prefix, or empty) does NOT confirm, so
        // a stray POST can never forget the wrong node.
        assert!(!forget_confirmed(&body(
            id,
            "2222222222222222222222222222222222222222"
        )));
        assert!(!forget_confirmed(&body(id, "1111")));
        assert!(!forget_confirmed(&body(id, "")));
        // An empty node_id never confirms (even if confirm is also empty).
        assert!(!forget_confirmed(&body("", "")));
    }

    // ---- CLUSTER REBALANCE DRYRUN plan parsing (#361) ----

    /// Build ONE node row the way the engine sends it over the console's RESP2 link: a
    /// FLAT `[key, value, ...]` array (the RESP2 degrade of a RESP3 map). `slots_to_move`
    /// is the signed `target - current` the engine computes.
    fn node_row(id: &str, current: i64, target: i64) -> RespValue {
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"node".to_vec())),
            RespValue::Bulk(Some(id.as_bytes().to_vec())),
            RespValue::Bulk(Some(b"current_slots".to_vec())),
            RespValue::Integer(current),
            RespValue::Bulk(Some(b"target_slots".to_vec())),
            RespValue::Integer(target),
            RespValue::Bulk(Some(b"slots_to_move".to_vec())),
            RespValue::Integer(target - current),
        ])
    }

    #[test]
    fn rebalance_plan_parses_a_skewed_three_node_reply() {
        // ID0 owns the whole space; ID1/ID2 are empty. Balanced targets even out.
        let reply = RespValue::Array(vec![
            node_row("id0", 16384, 5462),
            node_row("id1", 0, 5461),
            node_row("id2", 0, 5461),
        ]);
        let plan = parse_rebalance_plan(&reply).expect("a well-formed reply parses");
        assert!(
            plan.ok && plan.dry_run,
            "the plan is always a read-only dry-run"
        );
        assert!(!plan.balanced, "a skewed cluster is not balanced");
        assert_eq!(plan.targets.len(), 3);
        assert_eq!(plan.targets[0].node, "id0");
        assert_eq!(
            plan.targets[0].slots_to_move,
            5462 - 16384,
            "id0 sheds (negative)"
        );
        assert_eq!(
            plan.targets[1].slots_to_move, 5461,
            "id1 receives (positive)"
        );
        // total = sum of POSITIVE deltas = 5461 + 5461 = 10922, which equals |id0 delta|
        // (16384 - 5462) by conservation: each relocating slot counted exactly once.
        assert_eq!(plan.total_slots_to_move, 10922);
    }

    #[test]
    fn rebalance_plan_of_a_balanced_reply_reports_no_moves() {
        let reply = RespValue::Array(vec![
            node_row("id0", 5461, 5461),
            node_row("id1", 5462, 5462),
            node_row("id2", 5461, 5461),
        ]);
        let plan = parse_rebalance_plan(&reply).expect("parses");
        assert!(plan.balanced, "all-zero moves is balanced");
        assert_eq!(plan.total_slots_to_move, 0);
    }

    #[test]
    fn rebalance_plan_field_order_is_not_assumed() {
        // The engine could reorder the map fields; lookup is by key, so it still reads.
        let row = RespValue::Array(vec![
            RespValue::Bulk(Some(b"slots_to_move".to_vec())),
            RespValue::Integer(-3),
            RespValue::Bulk(Some(b"target_slots".to_vec())),
            RespValue::Integer(7),
            RespValue::Bulk(Some(b"current_slots".to_vec())),
            RespValue::Integer(10),
            RespValue::Bulk(Some(b"node".to_vec())),
            RespValue::Bulk(Some(b"zed".to_vec())),
        ]);
        let plan = parse_rebalance_plan(&RespValue::Array(vec![row])).expect("parses");
        assert_eq!(plan.targets[0].node, "zed");
        assert_eq!(plan.targets[0].current_slots, 10);
        assert_eq!(plan.targets[0].target_slots, 7);
        assert_eq!(plan.targets[0].slots_to_move, -3);
    }

    #[test]
    fn rebalance_plan_rejects_non_array_and_incomplete_rows() {
        // Defensive totality: a real node `-ERR` (e.g. cluster support disabled) is
        // intercepted UPSTREAM as a `NodeError::Command` (a 502 with the node's text) and
        // never reaches the parser, but the parser must still be total, so a stray Error
        // variant safely yields `None` rather than panicking.
        assert!(
            parse_rebalance_plan(&RespValue::Error(b"ERR cluster support disabled".to_vec()))
                .is_none()
        );
        // A row MISSING `target_slots` fails the WHOLE parse (None), never a partial row.
        let bad_row = RespValue::Array(vec![
            RespValue::Bulk(Some(b"node".to_vec())),
            RespValue::Bulk(Some(b"x".to_vec())),
            RespValue::Bulk(Some(b"current_slots".to_vec())),
            RespValue::Integer(1),
            RespValue::Bulk(Some(b"slots_to_move".to_vec())),
            RespValue::Integer(0),
        ]);
        assert!(parse_rebalance_plan(&RespValue::Array(vec![bad_row])).is_none());
    }
}
