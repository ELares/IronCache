<!--
  SPDX-License-Identifier: MIT OR Apache-2.0
-->
# IronCache Console: threat model and security sign-off (#364)

This is the security sign-off for the `ironcache-console` (epic #352): the
separate monitoring server that discovers an IronCache deployment, aggregates a
cluster-wide view, and serves a dashboard, staying OUT of the client-to-shard
data path. It records the trust boundary, the assets, the controls that are in
place (with where they live), and the gates that MUST hold before the console is
exposed beyond loopback.

## What the console is, and its trust boundary

The console is a read-mostly control-plane component. It:

- connects OUTBOUND to each IronCache node over RESP (AUTH + `INFO` / `SLOWLOG` /
  `CLIENT LIST`) and, for trend history, OUTBOUND to a Prometheus HTTP API;
- serves INBOUND a small HTTP surface: the static dashboard (`/`, `/app.css`,
  `/app.js`), a JSON API (`/api/*`), and ops probes (`/livez`, `/readyz`,
  `/metrics`).

It never sits between a client and a shard, and it holds no cache data of its
own; its sensitive state is (a) the node credential it authenticates with and
(b) the node internals it reads (key names via slowlog argv, client IPs, node
addresses, version, memory/keyspace figures).

## Assets and adversaries

- Assets: the node ACL credential; the cluster-internal data the console reads
  (PII-adjacent: key names, client IPs); the integrity of what an operator sees.
- Adversaries: an unauthenticated network party that can reach the console; an
  operator without the privileged role; a COMPROMISED or hostile IronCache node
  or Prometheus the console talks to; a malicious dashboard input (XSS).

## Controls in place

### AuthN / AuthZ (three-tier RBAC, #360)
- Tiers OPEN < PRIVILEGED_READ < ADMIN (`src/auth.rs`). The route map is an exact
  OPEN allow-list and FAILS CLOSED: only `/api/health`, `/api/cluster` (aggregate
  totals + up/down counts only), and `/api/openapi.json` are OPEN; every other
  `/api/*` (nodes, nodes/{addr}, slowlog, clients, keyspace) and any
  unknown/trailing-slash path defaults to PRIVILEGED_READ. ADMIN is reserved for
  phase-2 management (#371); no route grants it today.
- Bearer-token auth: `read_token` grants OPEN+PRIVILEGED_READ, `admin_token`
  grants all. Tokens are compared in CONSTANT time (`ironcache_runtime::
  constant_time_eq`), held in `Zeroizing`, redacted in `Debug`, and never logged.
  Header-based, so the API is CSRF-safe by construction.
- Safe-by-default keyed off the bind: tokens configured => ENFORCE; no token +
  loopback => dev (serve all, with a warning); no token + NON-loopback => OPEN
  only (privileged returns 401 + a boot warning). PII is never served on an
  exposed bind without a token. Wildcard / unknown-host binds classify as
  non-loopback (fail closed).
- The dashboard is auth-aware (`ui/app.js`): it sends the operator token as a
  Bearer header (stored in `sessionStorage`, never in a URL/DOM/log) and reveals
  a sign-in panel on a privileged 401.

### "Read-only is not safe" (PII tiering, #360)
The slowlog argv (key names, sometimes values), `CLIENT LIST` (client IPs,
names), full `INFO` (node addresses), and the key/keyspace views are
PRIVILEGED_READ, not OPEN. The OPEN `/api/cluster` carries only aggregate numbers
and up/down counts, no identifying string.

### Outbound SSRF / injection defense (#356, #369)
- The Prometheus base URL comes ONLY from server config (`prometheus_url`), never
  from request input.
- `/api/timeseries` allows only a bare `ironcache_*` / `ironcache_console_*`
  metric name (no raw PromQL, label matchers, function calls, or `&query=`
  injection); the console builds the PromQL itself and URL-encodes it; range/step
  are clamped.
- The history HTTP client (`src/httpclient.rs`) does NOT follow redirects (a 3xx
  cannot pivot it to another host) and REJECTS connecting to link-local /
  cloud-metadata addresses (169.254.0.0/16 incl 169.254.169.254, fe80::/10, and
  the IPv4-mapped/compatible IPv6 forms) by screening the RESOLVED IP (the
  screened address is the one connected to: no TOCTOU; alternate-encoded and
  DNS-name-to-metadata targets are caught post-resolution), while still allowing
  the in-VPC RFC1918 Prometheus.

### Credential handling
- The node password is read from a FILE referenced by config
  (`node_password_file`), never inline; held in `Zeroizing`, redacted in `Debug`,
  never logged, and never serialized into any `/api/*` response or error body.

### Availability / hostile-peer bounds
- Every node connect + every read is bounded by an explicit timeout, so a down or
  never-replying node/Prometheus times out rather than hanging the poller (the
  regression guard for a prior production hang).
- The RESP reply parser caps recursion depth + array/bulk sizes; the HTTP client
  caps the response body size and decodes Content-Length + chunked safely; the
  inbound responder bounds the request (a whole-request deadline against
  slowloris, an 8 KiB header cap, a connection-concurrency cap). A hostile node or
  Prometheus cannot OOM, hang, or stack-overflow the console.

### Browser surface (#359, #369)
- The dashboard is framework-free vanilla JS with NO external/CDN fetch. Every
  server-supplied string reaches the DOM via `textContent`/`createTextNode` only
  (no `innerHTML`), so a compromised node's slowlog argv / client fields cannot
  XSS the dashboard.
- The UI responses carry a strict CSP (`default-src 'self'; base-uri 'none';
  frame-ancestors 'none'; object-src 'none'`), plus `X-Content-Type-Options:
  nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`. The `/api/*`
  responses carry `nosniff` + `Cache-Control: no-store`.

## Sign-off gates

| Gate | Status |
| --- | --- |
| No PII in the OPEN tier | MET (`/api/cluster` aggregate-only, #360) |
| No `+@all` node user | PENDING: requires the least-privilege `console_monitor` ACL user (#367, infra). Until it lands the console must use a scoped user, NOT `superuser`. |
| Step-up auth for ADMIN actions | N/A today (no ADMIN action exists; gating lands with #371) |
| SSRF allowlisting | MET (#356 metric allowlist + #369 no-redirect + metadata-IP block) |
| Console not internet-reachable | MET by default (loopback bind). Exposure requires the deployment gate below. |
| Credential blast radius bounded | PARTIAL: zeroized/redacted/least-tier-on-the-console-side; the NODE-side least-privilege user is #367. |

## Residual risks and the deployment gate (MUST hold before exposing the console)

1. NODE TLS: the console-to-node RESP link is plaintext today (the prod node is
   plaintext); the optional node TLS uses a fixed cluster SNI and is not yet a
   verified per-host link. Until that lands, the console MUST reach nodes over a
   trusted network only.
2. HTTPS to Prometheus is deferred (HTTP only, in-VPC); the console MUST reach
   Prometheus over a trusted network only.
3. LEAST-PRIVILEGE NODE USER (#367): provision and use the read-only
   `console_monitor` ACL user; do NOT wire `superuser` into the console.
4. EXPOSURE (#369 deployment): the console must run behind a VPN-locked,
   SG-restricted load balancer, reachable by
   operators only, never the public internet or the data-client network. When
   exposed, a token MUST be configured (the non-loopback no-token posture serves
   OPEN only, but a real deployment configures the read/admin tokens).

## Sign-off

The CODE-side security controls (RBAC, constant-time token auth, SSRF/injection
defense, PII tiering, credential hygiene, hostile-peer bounds, XSS-safe UI + CSP +
security headers) are implemented and adversarially reviewed. The console is safe
to run on its loopback default and on a trusted network with the tokens
configured. It is NOT cleared for public/internet exposure until the deployment
gate above (the least-privilege node user #367 and the VPN-locked exposure #369)
is satisfied in the infrastructure repo.
