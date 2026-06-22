# Changelog

All notable changes to IronCache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will
adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it ships a
release.

## [Unreleased]

### Security

- IronCache Console web hardening (issue #369): defense-in-depth on the Prometheus
  HTTP client and the API surface. The history HTTP client no longer follows
  redirects (a 3xx can no longer pivot the console to a different host), and it
  rejects connecting to link-local / cloud-metadata addresses (169.254.0.0/16
  incl. 169.254.169.254, fe80::/10, and the IPv4-mapped-IPv6 forms) by checking
  the RESOLVED IP, while still allowing the in-VPC RFC1918 Prometheus. The JSON
  `/api/*` responses now carry `X-Content-Type-Options: nosniff` and
  `Cache-Control: no-store` so the sensitive data is not MIME-sniffed or cached.
  These complement the existing controls (server-config-only Prometheus URL, the
  metric allowlist, the three-tier RBAC, and the strict UI CSP). The deployment
  hardening (VPN-locked load balancer, the least-privilege node ACL user #367)
  remains infra follow-up.

### Added

- IronCache Console: a new `ironcache-console` crate and binary (epic #352,
  issue #353), a SEPARATE server from the data-plane that will discover an
  IronCache deployment, aggregate a cluster-wide view, and serve a monitoring
  dashboard while staying out of the data path. This first slice is the skeleton:
  layered config (TOML plus `IRONCACHE_CONSOLE_*` env plus CLI), structured
  tracing, and a bounded hand-rolled HTTP responder serving `/livez`, `/readyz`,
  and the console's own `/metrics` (so the monitor can be monitored: poll
  success/failure counters and a last-successful-poll age gauge). Node
  acquisition, the single-node topology view, aggregation, the REST API, the UI,
  and TLS land in later PRs (#355, #366, #356, #358, #359).
- IronCache Console node acquisition and the single-node snapshot (issues #355,
  #366). The console now polls a seed node on an interval and publishes a
  topology view. New modules: a minimal RESP2 reply parser (`resp`); an async
  RESP `NodeClient` (`node`) that connects with `TCP_NODELAY`, optionally `AUTH`s
  as a least-privilege ACL user with the password read from `node_password_file`
  at connect time (never logged), and issues admin commands, with EVERY connect
  and EVERY read bounded by an explicit timeout so a down or never-replying node
  can never hang the poller (the regression guard for a prior production hang); an
  `INFO` parser (`info`) that extracts the dashboard fields plus a total key count
  and keeps the full raw map for version skew; an `acquire` / `snapshot` model
  (`snapshot`) that folds connect plus PING plus INFO into a `NodeSnapshot` (a
  down node yields `reachable=false` with an error, never a panic) and a
  single-node `Topology` (Standalone, with room for a clustered mode); and a poll
  loop (`poll`) that records the success/failure self-metrics, holds the latest
  topology in a shared cell readable by the HTTP surface, and flips `/readyz` to
  ready only after the FIRST successful poll (so the console reads not-ready until
  it has real data). New config fields `connect_timeout_secs` and
  `op_timeout_secs` (TOML, `IRONCACHE_CONSOLE_*` env, both default 5). Optional
  node TLS is wired through the runtime crate's cluster TLS client (verified
  against a configured CA); it uses a fixed SNI today, so full per-host SNI and
  mTLS for the console-to-node link are deferred to #369, and the plaintext path
  is the fully supported v1 path. A minimal `/debug/topology` JSON route exposes
  the current view; the public REST API lands in #358.
- IronCache Console security and correctness hardening. The RESP reply parser now
  caps nesting depth, declared array element count, and declared bulk length (with
  checked arithmetic) so a hostile or compromised node cannot stack-overflow the
  process or amplify work / overflow with an absurd count or length. Node TLS now
  REQUIRES peer verification by default: with TLS on and no CA the console refuses
  to boot unless the operator EXPLICITLY sets the new
  `node_tls_insecure_skip_verify` (TOML, `IRONCACHE_CONSOLE_*` env, default false),
  which then runs encrypted-but-unverified with a loud warning, closing the prior
  silent accept-any-certificate path. The node password is held in a zeroized
  buffer (scrubbed on drop) with a redacted Debug so it is never logged or placed
  in an error. The unauthenticated `/debug/topology` recon route is now gated OFF
  by default behind the new `enable_debug_routes` flag (TOML,
  `IRONCACHE_CONSOLE_*` env), to move behind the privileged/auth tier (#360/#369)
  before exposure. PING now verifies the reply is PONG (or OK) rather than
  accepting any reply.
- IronCache Console REST API (issue #358) and richer per-node acquisition
  (finishing #355). Node acquisition now also fetches `SLOWLOG GET 128` and
  `CLIENT LIST` per node, parsed into typed `SlowlogEntry` and `ClientInfo`
  values (the client info keeps a raw map for unmodeled fields). Each rich
  section is RESILIENT: a per-section timeout, parse fault, or ACL denial records
  that section's error and yields a degraded snapshot, never failing the whole
  acquire or flipping the node to unreachable. A new JSON REST API hangs off the
  existing bounded HTTP responder (the whole-request deadline, the size cap, and
  the concurrency permit all still apply): `GET /api/health`, `/api/cluster`,
  `/api/nodes`, `/api/nodes/{addr}` (the addr is URL-decoded; 404 on an unknown
  node), `/api/slowlog`, `/api/clients`, `/api/keyspace`, and a hand-written,
  valid `/api/openapi.json` (OpenAPI 3.0). Data endpoints return 503 with a JSON
  error before the first successful poll. Responses are rendered with the new
  `serde_json` dependency (pure-Rust, on the existing license allowlist), and the
  response types derive `serde::Serialize`. The `/api/*` surface exposes node
  internals (addresses, slowlog argv = key names, client IPs); it is
  unauthenticated today and relies on the loopback default bind, and an inline
  SECURITY note marks that it must move behind the auth/RBAC tier (#360) and
  VPN-locked exposure (#369) before the console is exposed. The interim
  hand-rolled `/debug/topology` JSON route and its `enable_debug_routes` config
  flag are REMOVED, superseded by the real `/api/*` surface.
- IronCache Console authentication and three-tier RBAC, enforced in the BACKEND
  (issue #360). The `/api/*` surface is split into three tiers because read-only
  is NOT safe: the slowlog argv carries KEY NAMES, the client list carries client
  IPs (PII), and the node list carries node addresses. OPEN (`/api/health`,
  `/api/cluster` aggregate totals + node up/down counts, `/api/openapi.json`) is
  safe to serve unauthenticated; PRIVILEGED_READ (`/api/nodes`,
  `/api/nodes/{addr}`, `/api/slowlog`, `/api/clients`, `/api/keyspace`) exposes
  addresses, key names, and client IPs; ADMIN is reserved for phase-2 management
  verbs (#371, none today). Tokens are presented in the
  `Authorization: Bearer <token>` HEADER (CSRF-safe by construction, no cookie):
  two new config fields `read_token` (grants OPEN + PRIVILEGED_READ) and
  `admin_token` (grants every tier), each via TOML, `IRONCACHE_CONSOLE_READ_TOKEN`
  / `IRONCACHE_CONSOLE_ADMIN_TOKEN` env, NEVER logged and shown only as `(set)` /
  `(none)` in the `config` dump. A presented token is compared in CONSTANT TIME
  (reusing `ironcache-runtime`'s `constant_time_eq`, never `==` on a secret) and a
  wrong token is treated as anonymous (401, not 403, so the response does not
  confirm a valid-but-insufficient token format). The SAFE-BY-DEFAULT posture is
  keyed off the bind: a token configured ENFORCES the tier check; no token on a
  LOOPBACK bind serves all tiers (the historical dev mode) with a one-time boot
  warning; no token on a NON-loopback (exposed) bind serves OPEN only and returns
  401 on privileged routes (never silently leaks PII), with a prominent boot
  warning. The gate runs in the request path around the API handler, so the
  privileged data is never produced for an unauthorized request, and the route
  classification FAILS CLOSED: anything not on the explicit OPEN allow-list
  (including `/api/nodes/{addr}`, an unknown endpoint, or a trailing-slash variant
  of an OPEN route) is PRIVILEGED_READ, so no path can evade the gate. The UI and
  probe routes (`/livez`, `/readyz`, `/metrics`) are not gated (the SPA is static
  and itself calls the gated API). FOLLOW-UP: the UI login flow that sends the
  token from the browser is deferred; on the loopback dev default the existing
  dashboard keeps working.
- IronCache Console history layer (issue #356): historical time series behind a
  pluggable source. A new `GET /api/timeseries?metric=<name>&range=<seconds>&step=<seconds>`
  endpoint serves a metric's samples over a time window as JSON. The source sits
  behind a new async `HistorySource` trait (the seam an embedded ring-buffer
  source, #370, can implement later) with a `PrometheusSource` adapter that queries
  Prometheus's `query_range` HTTP API and maps the `matrix` result into a
  `TimeSeries` (label set plus `(unix_ts, value)` points). The adapter uses a new
  minimal, hand-rolled HTTP/1.1 GET client (`httpclient`), built in the same style
  as the RESP node client and the metrics HTTP server (a tokio `TcpStream`, no
  hyper/reqwest), so the static musl build stays pure-Rust and NO new dependency is
  added. The client is HARD bounded: the connect has a connect timeout and the
  response read has a read timeout, so a down or never-replying Prometheus TIMES
  OUT promptly rather than hanging (the same discipline as the node client); it
  handles both `Content-Length` and chunked `Transfer-Encoding` bodies and enforces
  a hard response-size cap so a hostile or huge reply cannot drive an unbounded
  allocation. HTTPS to Prometheus is deferred (the in-VPC Prometheus is reached
  over plaintext, and the runtime crate's TLS client presents a fixed cluster SNI
  unsuitable for an arbitrary host); an `https://` URL is rejected rather than
  downgraded. SECURITY (SSRF / PromQL injection): the Prometheus base URL comes
  ONLY from server config (`prometheus_url`), never from request input, and the
  `metric` parameter is allowlisted to a bare `ironcache_*` / `ironcache_console_*`
  name (raw PromQL, label matchers, function calls, and `&query=` injection are
  rejected with 400); the console builds the PromQL itself from that bare name and
  URL-encodes it. The `range`/`step` are parsed and clamped to bounds so a request
  cannot demand an unbounded series. The endpoint returns 503 when no
  `prometheus_url` is configured, 400 on a missing or disallowed metric or a bad
  numeric parameter, 502 on a source/transport failure, and 200 with the series
  otherwise. The window's "now" is read through the `ironcache-env` clock seam, not
  the system clock directly.
- IronCache Console dashboard SPA (issue #359). A functional, API-driven
  monitoring dashboard served by the console's OWN HTTP responder at `GET /`,
  with the stylesheet and script at `GET /app.css` and `GET /app.js`. The UI is
  plain HTML plus CSS plus vanilla JavaScript: no npm, no framework, no build
  step, and no external CDN, keeping the static-musl pure-Rust posture. The three
  assets are embedded in the binary with `include_str!` (a new `assets` module)
  and served off the same bounded responder the probes and the `/api/*` surface
  use (the whole-request deadline, the size cap, and the concurrency permit still
  apply). CSS and JS are SEPARATE files (never inline) so the served pages run
  under a strict `Content-Security-Policy: default-src 'self'` with NO
  `unsafe-inline`; the HTML, CSS, and JS responses also carry
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and
  `Referrer-Policy: no-referrer`. The dashboard polls `/api/cluster`,
  `/api/nodes`, `/api/slowlog`, `/api/clients`, `/api/keyspace`, and `/api/health`
  on load and every 5 seconds, rendering the cluster totals, a per-node table, a
  slowlog table, a clients table, and a keyspace panel, plus a header with the
  deployment mode, nodes reachable / total, the last-poll age (computed from
  `last_poll_unixtime` against the browser clock), and the console version. It
  handles the pre-first-poll 503 gracefully (a "waiting for the first node poll"
  state that keeps retrying) and a fetch error (a banner while keeping the last
  good data on screen). Every server-supplied string (the slowlog argv and the
  client fields are attacker-influenceable via a compromised node) reaches the
  DOM only through `textContent` / `createTextNode`, never `innerHTML`, so there
  is no XSS sink. An inline SECURITY note marks that the UI is unauthenticated
  today behind the loopback default bind and must move behind the auth/RBAC tier
  (#360) and VPN-locked exposure (#369) before exposure.
- IronCache Console dashboard UI auth (the UI follow-up to #360 / #359). The
  dashboard is now auth-aware: it reads an operator token from `sessionStorage`
  (NOT `localStorage`, so it clears when the tab closes) and, when present, sends
  it as `Authorization: Bearer <token>` on EVERY `/api/*` fetch. The OPEN routes
  (`/api/health`, `/api/cluster`) need no token, so the header and the cluster
  overview always render even when signed out. When a PRIVILEGED_READ route
  (`/api/nodes`, `/api/slowlog`, `/api/clients`, `/api/keyspace`) returns 401 the
  UI reveals a hidden sign-in panel and renders those panels as "sign in to view"
  instead of an error banner; a 403 (a token of an insufficient tier) renders
  "insufficient privileges". The sign-in panel is static markup (a password input
  plus Sign in / Sign out buttons) wired with `addEventListener` (no inline
  `onclick`, so the strict `Content-Security-Policy: default-src 'self'` still
  needs no `unsafe-inline`); the same-origin `/api/*` fetch is allowed by the
  policy's `default-src` fallback. The token is sent ONLY as a request header: it
  is never written to the DOM/markup, never placed in a URL or query string, and
  never logged. On the loopback dev default (no token configured) the privileged
  routes still return 200 with no token, so every panel renders without signing
  in. Served-bytes tests assert the login element ids and the password field are
  present and that `app.js` references `Authorization` / `Bearer` /
  `sessionStorage` / `addEventListener` while still carrying no `innerHTML` sink,
  no inline `onclick`, and no `localStorage`.
- Keyspace notifications (PROD-8, Redis `notify-keyspace-events`). On a successful
  mutation (and on a TTL expiry / a maxmemory eviction) the server PUBLISHes the
  Redis keyspace + keyevent events to `__keyspace@<db>__:<key>` (payload = the
  event name, e.g. `set`/`del`/`lpush`/`expire`/`expired`/`evicted`) and
  `__keyevent@<db>__:<event>` (payload = the key), through the EXISTING Pub/Sub
  fan-out, so SUBSCRIBE / PSUBSCRIBE (and cross-shard subscribers) receive them
  exactly like a client PUBLISH. Gated by the `notify-keyspace-events` flag string
  (`K` keyspace, `E` keyevent, the event-class letters `g$lshzxe...`, and `A` =
  `g$lshzxet`), parsed to a compact bitset with the canonical Redis parse/render;
  `CONFIG GET`/`SET notify-keyspace-events` round-trips the canonical flag string,
  and TOML (`notify_keyspace_events`) / `IRONCACHE_NOTIFY_KEYSPACE_EVENTS` seed it
  at boot. DISABLED by default (the empty flag string, the Redis default): the
  emit helper short-circuits on the disabled set BEFORE any work, so the write hot
  path is byte-identical and pays no cost until a non-empty flag is set. The
  `expired`/`evicted` events are wired into the active + lazy TTL reap paths and
  the maxmemory eviction paths. Stream (`t`) and module (`d`) classes are
  recognized for parity but never fire (IronCache has no streams/modules); the
  new-key (`n`) and key-miss (`m`) events are recognized in the flag string but
  not emitted this pass.
- The WRITE-SIDE replication guardrail (`min-replicas-to-write` /
  `min-replicas-max-lag`, ADR-0026): an owner REJECTS a write to a slot it owns
  with `-NOREPLICAS Not enough good replicas to write.` when fewer than
  `min_replicas_to_write` replicas are currently in sync (lag within
  `min_replicas_max_lag`), so an acknowledged write is known to be on at least
  that many replicas, bounding the failover loss window (the read side was
  already bounded by `replica_max_lag`). The primary's per-replica serve tasks
  maintain a node-level in-sync count as a single lock-free `AtomicUsize` (one
  relaxed load on the write path); the check is owned-write-only,
  cluster/raft-mode-only, and DISABLED by default (`min_replicas_to_write = 0`,
  the Redis default), so the write hot path is byte-unchanged at the default.
  Configurable via TOML / `IRONCACHE_MIN_REPLICAS_TO_WRITE` /
  `IRONCACHE_MIN_REPLICAS_MAX_LAG`.
- Fully automated versioning and releases, in two channels (RELEASING.md). A new
  `rolling-release` workflow publishes a calendar-versioned (`YYYY.MMDD.N`)
  GitHub Release on every push to `main`: four reproducible `cargo-zigbuild`
  tarballs (x86_64/aarch64, musl/glibc-2.17), a consolidated `SHA256SUMS`, and a
  keyless Sigstore build-provenance attestation, as a normal release so
  `releases/latest` tracks the newest build (`[skip release]` opts out). The
  published version is stamped into the binary via `IRONCACHE_BUILD_VERSION`
  (read by `option_env!` in `cli::BUILD_VERSION`, with a `build.rs`
  `rerun-if-env-changed` so a warm target re-stamps), so `ironcache --version`
  reports the build without touching `Cargo.lock` (pinned at `0.0.0`).
- The formal `v*` `release` workflow is now a working pipeline rather than a
  scaffold: a changelog gate (`scripts/ci/changelog-unreleased.sh`) that fails
  an empty-changelog release before building, a real CycloneDX SBOM export from
  the embedded `cargo-auditable` data (`auditable2cdx`, #123), a secret-gated
  minisign signature over `SHA256SUMS` (ADR-0020), a keyless Sigstore
  attestation, and `v0.*` prerelease marking. Fixed a `SHA256SUMS` bug where a
  `merge-multiple` artifact-download filename collision left three of the four
  tarballs unchecksummed; the consolidated file is now rebuilt from the tarballs
  and self-checked.
- First engine code (PR-1 "Boot + wire"): a Cargo virtual workspace and a
  minimal-but-real server that accepts RESP connections and answers the Tier-0
  connection commands. Seven crates: `ironcache-env` (the determinism Env seam,
  ADR-0003), `ironcache-protocol` (RESP2/RESP3 codec and the verbatim error
  catalog, PROTOCOL.md/ERRORS.md/ADR-0019), `ironcache-runtime` (the swappable
  Runtime trait and a shared-nothing tokio current-thread, per-core-pinned
  bootstrap with SO_REUSEPORT per-shard accept, RUNTIME.md/ADR-0002),
  `ironcache-config` (layered TOML config with human-size parsing, CONFIG.md),
  `ironcache-observe` (INFO sections and per-shard rollup counters,
  OBSERVABILITY.md), `ironcache-server` (the HELLO-driven connection state
  machine and Tier-0 dispatch: PING/HELLO/AUTH/SELECT/QUIT/RESET/CLIENT/COMMAND/
  INFO/ECHO), and the `ironcache` binary (clap subcommands server|cli|bench|
  check|config|upgrade, the redis-cli argv[0] alias, jemalloc global allocator
  per ADR-0006, graceful SIGINT/SIGTERM shutdown).
- Rust CI workflow (`.github/workflows/rust.yml`) with the docs-only guard idiom:
  fmt, clippy (pedantic, -D warnings), test (ubuntu + macos), MSRV 1.85, a musl
  static build, and grep-based invariant lints
  (`scripts/ci/check-rust-invariants.sh`) enforcing no-fork, the determinism
  Env-seam boundary, shared-nothing locks, and the SPDX header on every source.
- `deny.toml` (cargo-deny license allowlist, advisories, sources),
  `rust-toolchain.toml` (pinned stable channel), and a committed `Cargo.lock`.
- Project scaffolding: dual `MIT OR Apache-2.0` license, Code of Conduct,
  Contributing guide, Governance, and Security policy.
- The vision EPIC and the research and specification issue tree.
- Prior-art research corpus under `docs/research/`, the version-pinned
  `docs/prior-art/claims.yaml`, and the `docs/PRIOR_ART.md` survey.
- Pre-implementation audit (`docs/AUDIT.md`): every issue audited and given a
  verdict comment, 5 prior-art claims re-verified and corrected, 27 split
  sub-issues filed from 9 decomposed parents, and 36 coverage-gap issues filed.

### Security

- In-memory zeroization of plaintext secrets, defense-in-depth (#145, the last
  production-readiness follow-up). The long-lived `cluster_secret` (the one secret
  that cannot be reduced to a hash, since the peer handshake compares its literal
  bytes) is now held in a `Zeroizing<Vec<u8>>` inside `ClusterSecurity`, so it is
  scrubbed from the heap when the node tears down; and the transient plaintext copy
  a `CONFIG SET requirepass` materializes is a `Zeroizing<String>`, scrubbed the
  instant it is hashed to a digest at rest. TLS private keys are already zeroized by
  rustls itself (not double-handled). The `AUTH`/`HELLO AUTH`/`ACL SETUSER >pass`
  transient plaintext is documented as an accepted residual (it lives only in the
  shared/immutable decoded argument and the reused codec read buffer, which is
  `drain`-ed forward to preserve pipelined bytes; scrubbing it risks pipelining for
  marginal gain). The scope decision (what is and is not protected, and why) is in
  `SECURITY.md`, `docs/design/SECRETS.md`, and `docs/THREAT_MODEL.md`. Off the hot
  data path; the auth logic, the wire protocol, and the non-auth hot path are
  byte-unchanged. The `zeroize` crate was already in the lock transitively (rustls),
  so no new crate enters the supply chain.

### Changed

- Corrected 5 prior-art claims in `docs/prior-art/claims.yaml` after
  re-verification (provenance preserved via `verification.reaudited`).

### Fixed

- Cross-shard atomicity for spanning multi-key + move commands (PROD-9): a SILENT
  partial-apply safety bug. On a multi-shard node (the default; shards == cores) a
  2-key src/dst command (RENAME/RENAMENX/COPY/SMOVE/LMOVE/RPOPLPUSH) or a strided
  multi-key command (MSETNX/LMPOP/ZMPOP) whose keys hashed to DIFFERENT internal
  shards used to fall through to the HOME shard and operate on ONLY the home-owned
  subset of the keys, applying a PARTIAL result SILENTLY (a spanning RENAME saw a
  sibling-shard `src` as absent and errored, or wrote `dst` onto the wrong shard --
  a silent lost write; a spanning MSETNX checked + set only the home keys and
  misreported its 1/0). The fix ends every silent partial: SMOVE / LMOVE /
  RPOPLPUSH and MSETNX now apply ATOMICALLY across the owner shards (a home-core
  gather + validate-then-commit, each sub-op a single deadlock-free deterministic
  hop -- the element/member is held on the home core between the source read and
  the dest write so it can never be lost, and MSETNX scans every key's existence
  before any write); RENAME / RENAMENX / COPY / LMPOP / ZMPOP / SORT...STORE (which
  need a value-object cross-shard transfer the engine does not expose yet) now
  FAIL-LOUD with a clear error naming the hash-tag co-location remedy instead of
  silently partial-applying. Co-located (same-shard) and `shards == 1` invocations
  are byte-identical to the single-shard handler. Cross-shard MULTI/EXEC + WATCH
  were already fail-loud (the existing in-MULTI cross-shard + WATCH guards), so no
  silent transaction partial remained. Documented residual divergences from
  single-node Redis (no data loss, narrower than the silent partial they replace):
  a spanning move has a brief transient-visibility window (SMOVE: member momentarily
  in both sets; LMOVE/RPOPLPUSH: element momentarily in neither) but never loses an
  element; if the source-remove hop fails after the dest-add committed, SMOVE now
  compensates (removes from dest) and surfaces the error rather than reporting a
  clean move; and spanning MSETNX has a check-then-write window (a key created
  concurrently between the existence scan and the writes is overwritten) -- use a
  hash tag to co-locate keys for strict single-shard atomicity.
- Removed or relinked broken citations in issue bodies (#83, #88, #97).
