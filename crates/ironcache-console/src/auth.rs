// SPDX-License-Identifier: MIT OR Apache-2.0
//! Console authentication and three-tier RBAC (issue #360), enforced in the
//! BACKEND so the gate cannot be bypassed by a crafted client.
//!
//! ## Why read-only is NOT safe
//!
//! The `/api/*` surface exposes node internals that are sensitive even read-only:
//! the slowlog argv carries KEY NAMES, the client list carries client IPs (PII),
//! and the node list carries node addresses. So the API is split into three tiers
//! rather than a flat public/private:
//!
//!   * [`Tier::Open`]: aggregate, non-identifying facts (health, cluster totals
//!     and node up/down counts, the OpenAPI document). Safe to serve unauthed.
//!   * [`Tier::PrivilegedRead`]: anything that exposes addresses, key names, or
//!     client IPs (`/api/nodes`, `/api/nodes/{addr}`, `/api/slowlog`,
//!     `/api/clients`, `/api/keyspace`), plus the sensitive management READS
//!     (`/api/config`, `/api/keys`, `/api/keys/{k}`, `/api/pubsub/channels`,
//!     `/api/persistence`).
//!   * [`Tier::Admin`]: the node-level MANAGEMENT WRITES (#361): `CONFIG SET`, key
//!     CRUD (`POST`/`DELETE /api/keys/...`, expire/persist), the arbitrary command
//!     console (`POST /api/command`), pub/sub publish, ACL user management, and the
//!     persistence save. Every mutation requires the admin token, enforced
//!     SERVER-SIDE in the request path BEFORE the handler runs. The `/api/acl`
//!     READ is Admin too (it discloses the node's full user/permission set).
//!
//! ## Method matters for the tier (#361)
//!
//! A route's tier now depends on the HTTP METHOD as well as the path: a `GET
//! /api/config` is a `PrivilegedRead`, but a `POST /api/config` is `Admin`. The
//! gate consults [`route_tier_for_method`] (the method-aware mapping) so a write
//! verb can never inherit a read route's lower tier. The default is still FAIL
//! CLOSED: an unknown route is `PrivilegedRead`, and a non-GET on an unknown route
//! is `Admin` (a mutation must never default below the admin bar).
//!
//! ## Token model
//!
//! Tokens are presented in the `Authorization: Bearer <token>` HEADER, which is
//! CSRF-safe by construction (no cookie is read, so a cross-site form post cannot
//! ride an ambient credential). Two tokens are configured:
//!
//!   * `read_token`  grants `Open` + `PrivilegedRead`.
//!   * `admin_token` grants every tier.
//!
//! A presented token is compared in CONSTANT TIME (see [`token_matches`]) so an
//! attacker cannot recover it byte-by-byte from response timing, and a token is
//! NEVER logged or placed in an error.
//!
//! ## Safe-by-default posture (keyed off the bind)
//!
//! The posture depends on whether any token is configured and whether the listen
//! address is loopback (see [`AuthPolicy::resolve`] and [`authorize`]):
//!
//!   * tokens configured            -> ENFORCE: a request's tier is derived from
//!     its Bearer token; `Open` needs none, `PrivilegedRead`/`Admin` need a token
//!     that grants the tier (else 401 with no token / 403 with an insufficient
//!     one).
//!   * no tokens AND loopback bind  -> DEV mode: serve every tier (the historical
//!     loopback-trusted behavior), with a ONE-TIME boot warning.
//!   * no tokens AND non-loopback   -> EXPOSED: serve `Open` only; any privileged
//!     route returns 401 telling the operator to configure a token (never
//!     silently leak PII on an exposed bind).
//!
//! The gate is enforced in the request path ([`crate::http`]) around
//! [`crate::api::handle`], so it applies uniformly to every `/api/*` route.

use ironcache_runtime::constant_time_eq;
use zeroize::Zeroizing;

/// The RBAC tier a route requires, and (when derived from a token) the tier a
/// caller holds. Ordered least-to-most privileged: a higher tier grants every
/// lower one (see [`Tier::grants`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    /// Aggregate, non-identifying facts. Safe to serve without a token.
    Open,
    /// Exposes addresses, key names, or client IPs; needs the read or admin token.
    PrivilegedRead,
    /// The node-level management WRITES (#361) and the ACL read; needs the admin
    /// token. Every mutation the console can issue maps here.
    Admin,
}

impl Tier {
    /// Whether holding `self` grants access to a route that requires `required`.
    /// A higher tier grants every lower one (Admin grants PrivilegedRead grants
    /// Open). The derived `Ord` encodes the privilege order.
    #[must_use]
    pub fn grants(self, required: Tier) -> bool {
        self >= required
    }
}

/// The OPEN routes: aggregate, non-identifying facts that are safe to serve
/// without a token. EVERYTHING ELSE under `/api/` is privileged (fail closed), so
/// this allow-list is the ONLY way a route becomes Open: a new endpoint is
/// privileged until it is deliberately added here.
///
///   * `/api/health`       liveness/readiness/version/uptime.
///   * `/api/cluster`      aggregate totals + node up/down counts (no addresses).
///   * `/api/openapi.json` the static API document.
const OPEN_ROUTES: [&str; 3] = ["/api/health", "/api/cluster", "/api/openapi.json"];

/// The KNOWN privileged routes, documented for the reader and the route-map test.
/// They are NOT special-cased in [`route_tier`] (everything not in [`OPEN_ROUTES`]
/// is already `PrivilegedRead`); this constant records the intent and lets a test
/// assert each maps to `PrivilegedRead`. Test-only (the live mapping is the
/// fail-closed default), so it is `#[cfg(test)]`.
///
///   * `/api/nodes`, `/api/nodes/{addr}`  node addresses.
///   * `/api/slowlog`                      slowlog argv = KEY NAMES.
///   * `/api/clients`                      client IPs (PII).
///   * `/api/keyspace`                     per-db key counts.
#[cfg(test)]
const KNOWN_PRIVILEGED_ROUTES: [&str; 5] = [
    "/api/nodes",
    "/api/nodes/{addr}",
    "/api/slowlog",
    "/api/clients",
    "/api/keyspace",
];

/// The ADMIN read routes: management reads that disclose the node's FULL config or
/// user/permission set, sensitive enough to require the admin token even to read.
/// `/api/acl` lists every ACL user and their rules. (Mutations are handled by the
/// method-aware [`route_tier_for_method`], not this list.)
const ADMIN_READ_ROUTES: [&str; 1] = ["/api/acl"];

/// Map an `/api/*` request path (already query-stripped) to the tier a GET of it
/// requires. Equivalent to `route_tier_for_method("GET", path)`; kept for the
/// callers and tests that reason about the read tier of a path.
///
/// OPEN is the explicit allow-list [`OPEN_ROUTES`]; the DEFAULT for anything else
/// under `/api/` is `PrivilegedRead` (FAIL CLOSED): a new or unknown endpoint,
/// `/api/nodes/{addr}`, and even a trailing-slash variant of an Open route are all
/// treated as sensitive, so none can land in the Open path and evade the gate.
#[must_use]
pub fn route_tier(path: &str) -> Tier {
    route_tier_for_method("GET", path)
}

/// Map an `/api/*` request (METHOD + already-query-stripped PATH) to the tier it
/// requires. This is the live mapping the gate uses (#361).
///
/// The rules, in order:
///   * Any NON-GET, NON-HEAD method (`POST`, `DELETE`, `PUT`, ...) is a MUTATION
///     and requires `Admin`. This is the fail-closed default for writes: a write
///     verb can never inherit a read route's lower tier, even on an unknown path,
///     a trailing-slash variant, or a dynamic sub-path.
///   * A `GET`/`HEAD` of an [`OPEN_ROUTES`] path is `Open`.
///   * A `GET`/`HEAD` of an [`ADMIN_READ_ROUTES`] path is `Admin` (it discloses the
///     full config / ACL).
///   * Every OTHER `GET`/`HEAD` under `/api/` is `PrivilegedRead` (FAIL CLOSED).
#[must_use]
pub fn route_tier_for_method(method: &str, path: &str) -> Tier {
    let is_read = method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD");
    if !is_read {
        // Any mutation verb is Admin, regardless of path (fail closed for writes).
        return Tier::Admin;
    }
    if OPEN_ROUTES.contains(&path) {
        Tier::Open
    } else if ADMIN_READ_ROUTES.contains(&path) {
        Tier::Admin
    } else {
        Tier::PrivilegedRead
    }
}

/// The resolved authentication policy: the configured tokens (held zeroized so
/// they are scrubbed on drop) and whether the listen address is loopback, which
/// together decide the safe-default posture.
#[derive(Clone)]
pub struct AuthPolicy {
    /// The read token, if configured. Grants `Open` + `PrivilegedRead`.
    read_token: Option<Zeroizing<String>>,
    /// The admin token, if configured. Grants every tier.
    admin_token: Option<Zeroizing<String>>,
    /// Whether the console binds a loopback address (decides the no-token posture).
    loopback: bool,
}

// Hand-written so a token value can never reach a log or error through Debug.
impl std::fmt::Debug for AuthPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthPolicy")
            .field(
                "read_token",
                &self.read_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "<redacted>"),
            )
            .field("loopback", &self.loopback)
            .finish()
    }
}

/// The decision the gate returns for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Serve the request (the API handler runs).
    Allow,
    /// 401: no usable credential was presented for a gated route. The message is
    /// operator-facing and never names which token would have worked.
    Unauthorized(&'static str),
    /// 403: a VALID token was presented but it does not grant the required tier.
    Forbidden(&'static str),
}

impl AuthPolicy {
    /// Resolve the policy from the configured tokens and the bind classification.
    /// Empty/whitespace-only tokens are treated as UNSET (a blank token must not
    /// silently authenticate everyone); a token is otherwise taken verbatim.
    #[must_use]
    pub fn resolve(read_token: Option<&str>, admin_token: Option<&str>, loopback: bool) -> Self {
        let normalize = |t: Option<&str>| -> Option<Zeroizing<String>> {
            t.map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| Zeroizing::new(s.to_owned()))
        };
        AuthPolicy {
            read_token: normalize(read_token),
            admin_token: normalize(admin_token),
            loopback,
        }
    }

    /// Whether ANY token is configured (so the policy is in ENFORCE mode).
    #[must_use]
    pub fn enforcing(&self) -> bool {
        self.read_token.is_some() || self.admin_token.is_some()
    }

    /// Whether the bind is loopback (used for the no-token posture + boot logging).
    #[must_use]
    pub fn loopback(&self) -> bool {
        self.loopback
    }

    /// The highest tier a presented token grants, or `None` for no/unknown token.
    /// The admin token is checked first (it grants more); BOTH configured tokens
    /// are compared in constant time so a wrong token cannot be distinguished from
    /// "no such token" by timing. A token that matches NEITHER yields `None`
    /// (treated as anonymous), so a wrong token on a privileged route is a 401,
    /// not a 403 that would confirm the token format.
    #[must_use]
    fn token_tier(&self, presented: &str) -> Option<Tier> {
        let mut tier: Option<Tier> = None;
        if let Some(admin) = &self.admin_token {
            if token_matches(admin, presented) {
                tier = Some(Tier::Admin);
            }
        }
        if let Some(read) = &self.read_token {
            // Constant-time check the read token too, regardless of the admin
            // result, so the work (and timing) does not depend on which matched.
            // Admin outranks read, so only upgrade when no admin match was found.
            if token_matches(read, presented) && tier.is_none() {
                tier = Some(Tier::PrivilegedRead);
            }
        }
        tier
    }
}

/// Decide whether a request for a route of `required` tier, carrying the raw
/// `Authorization` header value `auth_header` (if any), is allowed.
///
/// Posture (see the module docs):
///   * ENFORCE (a token configured): `Open` is always allowed; a higher tier
///     needs a token that grants it. No token on a gated route -> 401; a valid
///     token of an insufficient tier -> 403; a token matching neither -> 401
///     (treated as anonymous, so the response does not confirm the token).
///   * NO token + loopback: every tier is allowed (dev mode).
///   * NO token + non-loopback: only `Open` is allowed; a gated route -> 401 with
///     guidance to configure a token (never silently expose PII).
#[must_use]
pub fn authorize(policy: &AuthPolicy, required: Tier, auth_header: Option<&str>) -> Decision {
    // Open routes are public in every posture (they carry no identifying data).
    if required == Tier::Open {
        return Decision::Allow;
    }

    if !policy.enforcing() {
        // No token configured: loopback is dev-trusted; a non-loopback (exposed)
        // bind must NOT serve a privileged route without auth.
        return if policy.loopback() {
            Decision::Allow
        } else {
            Decision::Unauthorized(
                "this endpoint exposes node internals and the console is bound to a non-loopback \
                 address; configure read_token or admin_token to access it",
            )
        };
    }

    // Enforcing: derive the caller's tier from the presented Bearer token.
    let Some(token) = auth_header.and_then(parse_bearer) else {
        return Decision::Unauthorized(
            "missing or malformed Authorization header; present 'Authorization: Bearer <token>'",
        );
    };
    match policy.token_tier(token) {
        // A token that matches neither configured token is treated as anonymous:
        // 401 (not 403), so the response does not confirm a valid-but-wrong-tier
        // token format.
        None => Decision::Unauthorized("invalid token"),
        Some(held) if held.grants(required) => Decision::Allow,
        Some(_) => Decision::Forbidden("the presented token does not grant the required tier"),
    }
}

/// Parse the value of an `Authorization` header into the bearer TOKEN, or `None`
/// when it is not a usable `Bearer` credential.
///
/// Accepts a case-insensitive `bearer` scheme (RFC 7235 auth schemes are
/// case-insensitive) followed by at least one space and a NON-EMPTY token. A
/// missing scheme, the wrong scheme, or an empty token after the scheme yields
/// `None` rather than panicking. Surrounding whitespace on the header value is
/// tolerated.
#[must_use]
pub fn parse_bearer(header_value: &str) -> Option<&str> {
    let value = header_value.trim();
    // Split once on the first run of whitespace into scheme + the rest.
    let (scheme, rest) = value.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = rest.trim();
    if token.is_empty() { None } else { Some(token) }
}

/// Compare a CONFIGURED token against a PRESENTED token in constant time, reusing
/// the runtime crate's `constant_time_eq` (the same compare the engine uses for
/// the cluster shared-secret handshake). A length difference short-circuits (a
/// token's length is not itself the secret), and equal-length inputs fold every
/// byte so the timing does not reveal the matching prefix; never `==` on a secret.
#[must_use]
fn token_matches(configured: &str, presented: &str) -> bool {
    constant_time_eq(configured.as_bytes(), presented.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_ordering_grants_lower_tiers() {
        assert!(Tier::Admin.grants(Tier::Admin));
        assert!(Tier::Admin.grants(Tier::PrivilegedRead));
        assert!(Tier::Admin.grants(Tier::Open));
        assert!(Tier::PrivilegedRead.grants(Tier::Open));
        assert!(Tier::PrivilegedRead.grants(Tier::PrivilegedRead));
        assert!(!Tier::PrivilegedRead.grants(Tier::Admin));
        assert!(Tier::Open.grants(Tier::Open));
        assert!(!Tier::Open.grants(Tier::PrivilegedRead));
        assert!(!Tier::Open.grants(Tier::Admin));
    }

    #[test]
    fn route_tier_maps_open_and_privileged() {
        for p in OPEN_ROUTES {
            assert_eq!(route_tier(p), Tier::Open, "{p}");
        }
        // Every documented privileged route maps to PrivilegedRead. The
        // `{addr}` placeholder stands for the dynamic route; check a concrete one.
        for p in KNOWN_PRIVILEGED_ROUTES {
            let concrete = if p == "/api/nodes/{addr}" {
                "/api/nodes/10.0.0.1:6379"
            } else {
                p
            };
            assert_eq!(route_tier(concrete), Tier::PrivilegedRead, "{concrete}");
        }
        // The OPEN allow-list and the documented privileged set are disjoint.
        for p in KNOWN_PRIVILEGED_ROUTES {
            assert!(!OPEN_ROUTES.contains(&p), "{p} must not be open");
        }
    }

    #[test]
    fn unknown_and_trailing_slash_routes_fail_closed_to_privileged() {
        // A trailing slash on a privileged route, a deeper path, and a wholly
        // unknown endpoint must NOT fall into the Open default (no gate bypass).
        for p in [
            "/api/nodes/",
            "/api/cluster/",
            "/api/health/",
            "/api/openapi.json/",
            "/api/bogus",
            "/api/nodes/a/b",
            "/api",
        ] {
            assert_eq!(route_tier(p), Tier::PrivilegedRead, "{p} must fail closed");
        }
    }

    #[test]
    fn mutations_require_admin_on_every_path() {
        // Any non-GET/HEAD verb is Admin, even on Open / unknown / trailing-slash
        // paths: a write can never inherit a read route's lower tier (#361).
        for method in ["POST", "DELETE", "PUT", "PATCH", "post", "delete"] {
            for path in [
                "/api/config",
                "/api/keys/foo",
                "/api/command",
                "/api/health",  // Open as a GET, but a write here is Admin.
                "/api/cluster", // ditto.
                "/api/acl/user/bob",
                "/api/persistence/save",
                "/api/bogus",     // unknown -> still Admin for a write.
                "/api/keys/foo/", // trailing slash -> still Admin.
            ] {
                assert_eq!(
                    route_tier_for_method(method, path),
                    Tier::Admin,
                    "{method} {path} must require Admin"
                );
            }
        }
    }

    #[test]
    fn management_reads_map_to_their_read_tier() {
        // Sensitive management reads are PrivilegedRead; the ACL read is Admin.
        for path in [
            "/api/config",
            "/api/keys",
            "/api/keys/foo",
            "/api/pubsub/channels",
            "/api/persistence",
        ] {
            assert_eq!(
                route_tier_for_method("GET", path),
                Tier::PrivilegedRead,
                "GET {path} must be PrivilegedRead"
            );
        }
        assert_eq!(route_tier_for_method("GET", "/api/acl"), Tier::Admin);
        // A HEAD is treated like a GET for the tier.
        assert_eq!(
            route_tier_for_method("HEAD", "/api/config"),
            Tier::PrivilegedRead
        );
        // The Open routes stay Open as a GET.
        for p in OPEN_ROUTES {
            assert_eq!(route_tier_for_method("GET", p), Tier::Open, "{p}");
        }
    }

    #[test]
    fn parse_bearer_handles_valid_missing_and_malformed() {
        assert_eq!(parse_bearer("Bearer abc"), Some("abc"));
        // Scheme is case-insensitive.
        assert_eq!(parse_bearer("bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("BEARER abc"), Some("abc"));
        // Surrounding and internal extra whitespace tolerated.
        assert_eq!(parse_bearer("  Bearer    abc  "), Some("abc"));
        // Missing token after the scheme.
        assert_eq!(parse_bearer("Bearer "), None);
        assert_eq!(parse_bearer("Bearer"), None);
        // Wrong scheme.
        assert_eq!(parse_bearer("Basic abc"), None);
        assert_eq!(parse_bearer("Token abc"), None);
        // Empty / whitespace-only header.
        assert_eq!(parse_bearer(""), None);
        assert_eq!(parse_bearer("   "), None);
        // No scheme at all (a bare token).
        assert_eq!(parse_bearer("abc"), None);
    }

    #[test]
    fn token_matches_is_correct() {
        assert!(token_matches("s3cr3t", "s3cr3t"));
        assert!(!token_matches("s3cr3t", "s3cr3T"));
        assert!(!token_matches("s3cr3t", "s3cr3"));
        assert!(!token_matches("", "x"));
    }

    #[test]
    fn blank_tokens_are_treated_as_unset() {
        // A blank or whitespace-only token must not put the policy into enforce
        // mode (that would silently authenticate everyone with an empty Bearer).
        let p = AuthPolicy::resolve(Some("   "), Some(""), true);
        assert!(!p.enforcing());
    }

    #[test]
    fn open_routes_allowed_in_every_posture() {
        for loopback in [true, false] {
            for (rt, at) in [(None, None), (Some("r"), None), (Some("r"), Some("a"))] {
                let p = AuthPolicy::resolve(rt, at, loopback);
                assert_eq!(authorize(&p, Tier::Open, None), Decision::Allow);
            }
        }
    }

    #[test]
    fn no_token_loopback_serves_privileged_dev_mode() {
        let p = AuthPolicy::resolve(None, None, true);
        assert_eq!(authorize(&p, Tier::PrivilegedRead, None), Decision::Allow);
        assert_eq!(authorize(&p, Tier::Admin, None), Decision::Allow);
    }

    #[test]
    fn no_token_non_loopback_blocks_privileged_serves_open() {
        let p = AuthPolicy::resolve(None, None, false);
        assert_eq!(authorize(&p, Tier::Open, None), Decision::Allow);
        assert!(matches!(
            authorize(&p, Tier::PrivilegedRead, None),
            Decision::Unauthorized(_)
        ));
        assert!(matches!(
            authorize(&p, Tier::Admin, None),
            Decision::Unauthorized(_)
        ));
    }

    #[test]
    fn read_token_grants_privileged_but_not_admin() {
        let p = AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), false);
        assert_eq!(
            authorize(&p, Tier::PrivilegedRead, Some("Bearer read-tok")),
            Decision::Allow
        );
        // Read token on an admin route -> 403 (valid token, insufficient tier).
        assert!(matches!(
            authorize(&p, Tier::Admin, Some("Bearer read-tok")),
            Decision::Forbidden(_)
        ));
    }

    #[test]
    fn admin_token_grants_all_tiers() {
        let p = AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), false);
        assert_eq!(
            authorize(&p, Tier::PrivilegedRead, Some("Bearer admin-tok")),
            Decision::Allow
        );
        assert_eq!(
            authorize(&p, Tier::Admin, Some("Bearer admin-tok")),
            Decision::Allow
        );
    }

    #[test]
    fn missing_token_when_enforcing_is_401() {
        let p = AuthPolicy::resolve(Some("read-tok"), None, true);
        assert!(matches!(
            authorize(&p, Tier::PrivilegedRead, None),
            Decision::Unauthorized(_)
        ));
        // A malformed header (no Bearer) is also a 401, not a panic.
        assert!(matches!(
            authorize(&p, Tier::PrivilegedRead, Some("garbage")),
            Decision::Unauthorized(_)
        ));
    }

    #[test]
    fn wrong_token_when_enforcing_is_401_not_403() {
        // A token matching neither configured token is anonymous -> 401, so the
        // response does not confirm a valid-but-insufficient token format.
        let p = AuthPolicy::resolve(Some("read-tok"), Some("admin-tok"), false);
        assert!(matches!(
            authorize(&p, Tier::PrivilegedRead, Some("Bearer nope")),
            Decision::Unauthorized(_)
        ));
    }

    #[test]
    fn read_only_configured_admin_route_with_read_token_is_403() {
        // Only a read token configured: it grants PrivilegedRead, and an Admin
        // route returns 403 (insufficient tier), not 401.
        let p = AuthPolicy::resolve(Some("read-tok"), None, false);
        assert!(matches!(
            authorize(&p, Tier::Admin, Some("Bearer read-tok")),
            Decision::Forbidden(_)
        ));
    }

    #[test]
    fn admin_only_configured_grants_privileged_too() {
        // Only an admin token configured: it grants every tier including
        // PrivilegedRead; a wrong token is still 401.
        let p = AuthPolicy::resolve(None, Some("admin-tok"), false);
        assert_eq!(
            authorize(&p, Tier::PrivilegedRead, Some("Bearer admin-tok")),
            Decision::Allow
        );
        assert!(matches!(
            authorize(&p, Tier::PrivilegedRead, Some("Bearer read-tok")),
            Decision::Unauthorized(_)
        ));
    }

    #[test]
    fn debug_never_prints_token_values() {
        let p = AuthPolicy::resolve(Some("super-secret"), Some("admin-secret"), true);
        let s = format!("{p:?}");
        assert!(!s.contains("super-secret"), "{s}");
        assert!(!s.contains("admin-secret"), "{s}");
        assert!(s.contains("redacted"), "{s}");
    }
}
