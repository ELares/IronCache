# Changelog

All notable changes to IronCache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will
adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it ships a
release.

## [Unreleased]

### Security

- IronCache Console security sign-off (issue #364): a threat model + control
  inventory at `crates/ironcache-console/SECURITY.md`. It records the trust
  boundary, the assets/adversaries, the code-side controls in place (three-tier
  RBAC + constant-time Bearer auth, PII tiering, SSRF/injection defense,
  credential hygiene, hostile-peer bounds, the XSS-safe UI + CSP + security
  headers), and the sign-off gates. Verdict: the code-side controls are
  implemented and reviewed; the console is safe on its loopback default and on a
  trusted network with tokens configured, but is NOT cleared for public exposure
  until the deployment gate (the least-privilege node ACL user #367 and the
  VPN-locked exposure #369) is satisfied in the infrastructure repo.
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

### Changed

- Cache-mode eviction-pool refill (`refill_evict_pool`) now selects the coldest
  candidates with a bounded max-heap of size `EVICT_POOL_CAP` instead of cloning
  every resident key into a vector and full-sorting it (the #285 "do first"
  follow-up). The per-refill cost drops from O(N) key allocations + O(N log N) sort
  to O(N log CAP) comparisons + O(CAP) memory, and a candidate warmer than the
  warmest kept is skipped without allocating its key, so refill no longer scales its
  allocations with the resident count N. This makes maxmemory eviction feasible at
  large resident sets without touching the table itself; the selected victim set is
  byte-identical to the prior full sort (same deterministic `(freq, scan_hash, key,
  db)` order, ADR-0003), pinned by a new N-greater-than-CAP equivalence test.
- `CLIENT PAUSE <ms> WRITE` is now genuinely write-only (reads + admin like SAVE,
  INFO and PING proceed; only writes are paused), matching Redis and fixing the
  ironcache-upgrade write-freeze that deadlocked its own SAVE (#388). The serve
  loop previously stalled on a write-flag-agnostic post-batch pause check, so a
  WRITE pause conservatively held the entire connection for the window, including
  reads, PING, INFO and SAVE. Because the upgrade issues `CLIENT PAUSE WRITE`
  then `SAVE`, the SAVE was held by the very pause it had set, timed out, and the
  upgrade safe-aborted. The pause stall is now per-command, applied right before
  each command is dispatched: an ALL pause holds every command (unchanged), a
  WRITE pause holds only writes (including EXEC of a write-containing transaction
  and any unknown command, conservatively), and `CLIENT UNPAUSE` is never held so
  a pause is always recoverable from the connection that set it. The default
  (no pause) path is unchanged: a single relaxed atomic load per command, with no
  clock read or command classification unless a pause is active.
- `ironcache upgrade` now write-freezes (`CLIENT PAUSE WRITE`) before the final
  save so no acknowledged write is lost across the upgrade (#388). Previously the
  upgrade was SAVE-first only, leaving a small window: between the SAVE completing
  and the old process dying at the restart, the still-living old process could
  acknowledge writes that were not in the snapshot, losing them on restart. The
  orchestrator now issues a node-wide `CLIENT PAUSE <ms> WRITE`, lets in-flight
  writes drain, then SAVEs (so the snapshot captures a state after which nothing
  acked exists outside it), then swaps and restarts; the old process dies at the
  restart so the freeze needs no explicit unpause, and the new process boots
  unpaused from the complete snapshot. An upgrade that aborts at ANY point while the
  freeze is active and the old process is still alive (a failed SAVE, a failed
  preflight, a failed binary swap, or a failed restart) issues a best-effort
  `CLIENT UNPAUSE` so production is never left write-frozen. The pause window is
  derived from
  `--health-timeout` plus a margin. The freeze is on by default; `--no-freeze`
  opts out and restores the prior SAVE-first-only behavior (for a read-mostly or
  rebuildable cache that accepts the tiny window). With no persistence configured
  the freeze is skipped (the restart loses data regardless, still gated on
  `--yes`).
- The `timeout` idle-client directive is now runtime-settable via `CONFIG SET
  timeout <secs>` / `CONFIG GET timeout` (it was boot-only, so changing it used to
  require a full server restart that dropped every connected client). The serve
  loop re-reads the live value from the runtime-config overlay at the top of each
  connection-loop iteration before its idle wait, so a `CONFIG SET timeout` takes
  effect immediately for already-connected clients on their next idle wait; `0`
  disables idle disconnection, and a negative or non-numeric value is rejected
  rather than silently coerced. This matches Redis, where `timeout` is a modifiable
  config.
- The eight collection-encoding thresholds (`hash-max-listpack-entries`,
  `hash-max-listpack-value`, `list-max-listpack-size`, `set-max-intset-entries`,
  `set-max-listpack-entries`, `set-max-listpack-value`, `zset-max-listpack-entries`,
  `zset-max-listpack-value`) are now LIVE via `CONFIG SET`/`CONFIG GET` (#40). They
  were previously accepted-but-IGNORED no-ops: `CONFIG SET` replied `+OK` while the
  store kept using the compiled defaults, and `CONFIG GET` echoed the compiled
  default rather than the value that was set (a lie). Now `CONFIG GET` reports the
  live value and the store reads the live threshold at the encoding-transition
  decision, so e.g. `CONFIG SET hash-max-listpack-entries 4` then creating a NEW
  hash with five fields stores it as a hashtable. A change affects FUTURE
  operations only, matching Redis: existing keys keep their encoding until the next
  rewrite (resident data is never re-encoded). `list-max-listpack-size` takes the
  signed Redis form (a negative `-1..-5` byte tier or a positive element count);
  the rest are positive counts / per-element byte caps, and an invalid value (zero
  or negative where not allowed, non-numeric) is rejected rather than silently
  ignored. The list listpack/quicklist transition is now a one-way ratchet (it no
  longer demotes when a list shrinks below the budget), matching Redis's quicklist
  and the hash/set/zset encoding ratchets. When a threshold is at its default the
  encoding behavior is byte-identical to before.
- `proto-max-bulk-len` is now runtime-settable via `CONFIG SET proto-max-bulk-len
  <bytes>` / `CONFIG GET proto-max-bulk-len` (it was a hardcoded 512 MB constant).
  The serve loop builds each connection's RESP decoder bulk-string limit from the
  live overlay, and the string-value-growth ceilings (APPEND / SETRANGE) and the
  bitmap bit-offset ceiling (SETBIT / GETBIT / BITFIELD) read the live value too,
  so the whole 512 MB-derived family of limits moves together. A change applies to
  newly-accepted connections and to subsequent value-growth edits; a human size
  (`512mb`) or a plain byte count is accepted, and `0` (which would reject every
  value) or a malformed value is rejected. The default keeps the prior 512 MB
  behavior byte-identical.
- `tcp-keepalive` is now supported and runtime-settable via `CONFIG SET
  tcp-keepalive <secs>` / `CONFIG GET tcp-keepalive` (it was absent: connections
  only had `TCP_NODELAY` set at accept). The accept path now enables `SO_KEEPALIVE`
  with the configured idle interval (Redis default 300 seconds; `0` disables it),
  so a dead peer behind a NAT/firewall that dropped state is detected and the
  half-open connection reaped. Read from the runtime overlay at accept, so a
  `CONFIG SET tcp-keepalive` applies to newly-accepted connections; an established
  connection keeps the option it was accepted with, matching Redis. Configurable at
  boot via TOML (`tcp_keepalive_secs`) and the `IRONCACHE_TCP_KEEPALIVE` env var.
- IronCache Console dashboard re-skin to the bespoke Butlr design system (issue
  #359). The generic dark dashboard is replaced with the real design language: a
  full-height sidebar (brand chip plus grouped nav) and topbar (page title, a
  cluster-state pill, a node selector, a Live toggle, refresh and theme-toggle
  icon buttons, an avatar), light and dark themes built from Butlr color tokens
  (the page/card surfaces, the ink primary text, the sparingly used red CTA, the
  lime accent), 16px rounded cards with a soft shadow, and a tabbed single-page
  layout. The real-data views are fully wired from the `/api/*` JSON: an Overview
  with five metric cards (throughput, hit rate, memory, keys, clients), an inline
  ops/second sparkline over a rolling 60-second buffer, and a per-node summary;
  plus the Nodes, Slowlog, Clients, and Keyspace tables. The not-yet-built views
  (Cluster, Replication, Shards, Console, Pub/Sub, Config, ACL) appear in the nav
  to match the design but show an honest empty-state card explaining why each is
  unavailable on a standalone node, with NO fabricated data. The fonts are
  self-hosted (SIL Open Font License 1.1; Hanken Grotesk and JetBrains Mono),
  embedded and served from `/assets/`, so the strict UI Content-Security-Policy
  (`default-src 'self'`) needs no CDN. All styling lives in `app.css` as classes
  with no inline style attributes or handlers, dynamic values (the per-node
  memory bars, the sparkline geometry, the theme) are driven through CSS custom
  properties and inline-SVG element nodes, and every server string still reaches
  the DOM only via textContent, so the strict CSP and the XSS-safe posture are
  unchanged. The token-based auth flow (sessionStorage, the privileged-401 sign-in
  reveal) is preserved. The icon set is a small hand-drawn inline-SVG family (no
  icon CDN).

### Added

- Console rebalance dry-run plan (issue #361, the management dry-run rail over
  engine #444): `GET /api/cluster/rebalance-plan` issues `CLUSTER REBALANCE DRYRUN`
  to the configured node and renders the per-node slot-balance plan as typed JSON
  (`{ok, dry_run, balanced, total_slots_to_move, targets:[{node, current_slots,
  target_slots, slots_to_move}]}`), so an operator sees the slot diff BEFORE any
  apply. READ-ONLY (it is a GET, the engine mutates nothing and refuses APPLY) but
  Admin-tier (a privileged cluster-management action, gated server-side via
  `ADMIN_READ_ROUTES`). The reply parser is pure and total: a non-array reply or a
  row missing a field maps to a `502`, and per-node fields are read by key (no field
  order assumed). `total_slots_to_move` counts the slots that change owner (the sum
  of the positive deltas, equal to the absolute negative sum by conservation).
  Documented in the OpenAPI (`RebalancePlanResponse`). The SPA panel and the
  mutating apply/failover actions remain.
- `CLUSTER REBALANCE [DRYRUN]` slot-balance planner (issue #371, the rebalance
  half): reports, for every known node, the slots it owns now, the balanced target
  (the assigned slots spread as evenly as possible across the members), and the
  signed `slots_to_move` (negative sheds, positive receives). The planner is PURE
  and READ-ONLY (it mutates nothing); `SlotMap::rebalance_plan` runs in O(slots +
  nodes) and its targets sum to the total assigned slots, so balancing moves slots
  without ever creating or dropping one (conservation-preserving). The slot-moving
  APPLY driver is a tracked follow-up, so `APPLY` is refused (rather than silently
  dry-running) and an unknown mode is the canonical syntax error. A single-node /
  non-cluster node reports one trivially balanced entry (zero moves), matching the
  other single-node `CLUSTER` projections. ACL: admin/dangerous-tier now, so the
  future APPLY needs no reclassification.
- Operator-triggered `CLUSTER FAILOVER` (issue #371, the failover half): a bare
  `CLUSTER FAILOVER` on an IN-SYNC replica now proposes a committed
  `ConfigCmd::PromoteReplica` through the leader (the same raft path every other
  cluster mutator uses), promoting this node to owner of the slots it replicates;
  the committed promotion atomically transfers ownership and bumps the config epoch
  (the split-brain fence). DATA-SAFETY: it is refused unless this node passes the
  EXACT in-sync gate the automatic promotion uses (`is_in_sync` within
  `replica_max_lag`, ADR-0026), so a manual failover can never promote a node the
  automatic path would not, and a stale replica is never promoted (which would lose
  committed writes). `FORCE` / `TAKEOVER` (which would bypass the in-sync and
  consensus safety) are refused. On a non-raft single node `CLUSTER FAILOVER` stays
  not-supported (no replicas). With the `CLUSTER REBALANCE` dry-run planner (above)
  now landed, the remaining #371 piece is the slot-moving APPLY driver.
- Console cluster aggregation: cache-specific cluster snapshot (issue #357, core):
  `GET /api/cluster` now carries the cluster-wide `hit_ratio` over the aggregate
  totals plus a `cluster_topology` object rolled up from the discovered structured
  topology (#354/#365): the committed config epoch, member count, the
  slot-ownership rollup (assigned-slot total + distinct owner count), and the raft
  consensus state. This is the cache-specific cluster view the console exists for
  (the non-goal fence: Grafana cannot express the committed-epoch slot map or the
  raft topology), distinct from the generic INFO totals. Coherent single-node values
  in standalone mode; `cluster_topology` is null when `/topology` discovery is not
  configured. The per-shard breakdown (multi-shard-only) and the cluster-wide
  replication-lag rollup (needs the #365 repl-fidelity follow-up) remain.
- Console embedded ring-buffer history (issue #370): in-memory trend history
  WITHOUT an external Prometheus, behind the SAME pluggable `HistorySource`
  interface as the Prometheus adapter (so it is swappable with no API change). The
  poll loop records each reachable node's headline `INFO` figures (memory, keys,
  clients, hits/misses, commands, evictions/expirations, mapped to the engine's
  `ironcache_*` metric names) into a bounded `(metric, node)` ring buffer each tick;
  `/api/timeseries` then serves trend panels from it. Memory is bounded TWO ways (a
  retention window in hours + a per-series point cap), the same `is_allowed_metric`
  allowlist guards queries, and a standalone/OSS deploy with no Prometheus now gets
  real short-window trends instead of empty panels. Enabled with
  `history_embedded_hours` / `IRONCACHE_CONSOLE_HISTORY_EMBEDDED_HOURS` (used only
  when `prometheus_url` is unset; Prometheus wins when both are configured).
- Console cluster discovery via the structured `/topology` endpoint (issue #354,
  core): the console now fetches the engine's `GET /topology` (#365) from a
  configured per-node HTTP admin URL (`node_http_url` /
  `IRONCACHE_CONSOLE_NODE_HTTP_URL`) and folds the typed cluster view (membership,
  slot-to-owner ranges bound to the committed epoch, raft leader/term/commit/voters)
  into the polled `Topology`, instead of parsing human-readable `CLUSTER NODES`/
  `SHARDS` text. Discovery is BEST-EFFORT (a fetch/parse miss leaves the cluster view
  absent and never affects node reachability) and works in STANDALONE mode (the
  engine's `/topology` returns a coherent single-node answer, so the console never
  blocks on a leader/epoch/slot-map that does not exist). The parser tolerates
  unknown future fields (the document is `schema_version`ed), so the per-replica
  endpoint/lag fidelity (#365 parts 3-4) and the multi-seed-failover / staleness-
  banner refinements are non-breaking follow-ups.
- Structured topology read endpoint `GET /topology` (issue #365, core): a versioned
  JSON document on the admin HTTP listener (alongside `/metrics` `/livez` `/readyz`)
  the console reads authoritative topology from (node identity + engine version,
  `cluster_mode`/`enabled`, membership, slot-to-owner map, committed epoch, and raft
  leader/term/commit/voters) WITHOUT parsing human-readable `CLUSTER NODES`/`SHARDS`
  text. It returns a COHERENT single-node answer in standalone mode (the node owns
  all 16384 slots at epoch 0 with itself as the only member), never an error, so the
  `cluster_enabled=false` production deployment still has a real topology read path.
  Read-only by construction (it only reads the live `SlotMap`/`RaftHandle` snapshots)
  and JSON is hand-rolled (no new dependency). The per-replica endpoint/offset/lag
  fidelity (#365 parts 3-4, which need the replication handshake + lag-model changes)
  is the documented follow-up; the `replication` object reports the node role for now.
- Per-shard `/metrics` labels (issue #362, the additive engine change for the
  console): the `/metrics` scrape now carries an `ironcache_shard_<name>{shard="i"}`
  series for every counter/gauge with per-shard (thread-per-core) meaning
  (connections, commands, evictions, expirations, keyspace hits/misses, connected
  clients, keyspace keys), so a console can render shard-level views. It is purely
  additive in a DISTINCT `ironcache_shard_*` namespace: the node-rollup families are
  byte-unchanged (no mixed-label double-count within a family), and the default path
  (no `--metrics-addr`) is byte-identical (the per-shard cells are read only at
  scrape time, off every command). Process-global gauges (uptime, allocator memory,
  raft) stay node-level, since they have no per-shard meaning.
- `ironcache-dashtable` crate: the standalone Dash-style extendible-hashing table,
  stage 1 of the #285 table rewrite (the algorithm core, validated in isolation
  with zero `unsafe` so `miri` is trivial). It implements the extendible directory
  (top-bit index, per-segment local depth), incremental segment SPLIT on overflow,
  directory DOUBLING when a split would exceed the global depth (a pointer-array
  copy, never a record rehash, so there is no power-of-two doubling trough), and the
  1-byte fingerprint that gates a lookup to skip non-matching slots. Pinned by a
  HashMap-oracle property test over a 30k-op deterministic insert/get/remove stream
  (forcing many splits + doublings) plus a 5k-key structural-growth test. The dense
  `unsafe` cache-line-packed layout, the bucketized/stash probing, and the
  feature-flagged store wiring + freq-in-object segment-local eviction are the
  later stages in `docs/design/DASHTABLE.md`; this crate is not yet wired into the
  store.
- HOTKEYS: the faithful Redis 8.6 hot-key tracking container (issue #428).
  `HOTKEYS START METRICS <count> [CPU] [NET] [COUNT k] [DURATION s] [SAMPLE ratio]
  [SLOTS ...]` begins a session that attributes per-command CPU microseconds and
  network bytes to the command's keys; `HOTKEYS GET` returns the session totals plus
  the top-K `by-cpu-time-us` / `by-net-bytes` lists (RESP3 map, RESP2 flat array, null
  when no session); `HOTKEYS STOP` halts but keeps the data; `HOTKEYS RESET` frees it.
  The top-K uses a bounded Space-Saving sketch (Metwally/Agrawal/El Abbadi 2005): O(1)
  amortized weighted update, O(cap) memory, no false negatives for a true heavy hitter.
  ACL `@admin`/`@slow`/`@dangerous`. The tracker is a node-level structure gated by ONE
  relaxed atomic when inactive, so the default deployment and the per-PR perf-gate
  (which run with tracking off) never touch the lock or the sketch; `SAMPLE ratio`
  further bounds the active cost via systematic sampling. CPU is measured as monotonic
  command-execution time (the same clock SLOWLOG/COMMANDSTATS use; no user/sys getrusage
  split). Single-shard-correct node-wide aggregation.
- CLIENT TRACKING REDIRECT, stage 4 (issue #409): `CLIENT TRACKING ON REDIRECT
  <client-id>` routes this connection's invalidations to a SECOND connection (the
  redirect target) instead of to itself. The target receives them as a Pub/Sub
  `message` on the well-known `__redis__:invalidate` channel (which it must
  `SUBSCRIBE`), so a RESP2 client (which has no push type) can be tracked: with a
  REDIRECT target, `CLIENT TRACKING ON` no longer requires RESP3. `REDIRECT 0` means
  no redirection; a non-zero target must be a live connection (else
  `The client ID you want redirect to does not exist`). `CLIENT TRACKINGINFO` reports
  the target id in its `redirect` field. NOLOOP still keys on the tracking client
  (the registrant), not the target. This completes CLIENT TRACKING (#409): default /
  BCAST / OPTIN / OPTOUT / REDIRECT are all supported. Single-shard-correct scope, as
  with the rest of tracking (the redirect target's subscription and the key's owner
  shard coincide when `shards == 1`).
- CLIENT TRACKING OPTIN/OPTOUT + CLIENT CACHING, stage 3 (issue #409):
  `CLIENT TRACKING ON OPTIN|OPTOUT` and `CLIENT CACHING YES|NO`. In OPTIN mode a
  read's keys are tracked only when the connection ran `CLIENT CACHING YES`
  immediately before; in OPTOUT mode every read is tracked except after
  `CLIENT CACHING NO`. The CACHING flag is one-shot (consumed by the next command).
  `CLIENT CACHING` is valid only in OPTIN/OPTOUT mode; OPTIN and OPTOUT are mutually
  exclusive and neither combines with BCAST. `CLIENT TRACKINGINFO` reports the
  `optin`/`optout` flags and the pending `caching-yes`/`caching-no` state. The
  no-tracking hot path is unchanged (the one-shot consume is a single `is_some`
  check when no flag is pending). `REDIRECT` remains the final staged follow-up.
- CLIENT TRACKING BCAST mode, stage 2 (issue #409): `CLIENT TRACKING ON BCAST
  [PREFIX prefix ...]` broadcast tracking. A BCAST connection does NOT register the
  keys it reads; instead its prefixes are registered once and EVERY changed key
  matching a prefix pushes an `["invalidate", [key]]` (sticky, not one-shot, unlike
  default mode). No `PREFIX` means the empty prefix (track all keys). `PREFIX`
  requires `BCAST`, and a client's prefixes may not overlap (one being a prefix of
  another is rejected). `CLIENT TRACKINGINFO` now reports the `bcast` flag and the
  prefix list; `FLUSHALL`/`FLUSHDB` flush BCAST clients too (keeping their prefix
  subscriptions). The prefix match is a linear scan over the registered prefixes
  (a radix tree is a documented refinement); the no-tracking hot path is unchanged.
  `OPTIN`/`OPTOUT`/`REDIRECT` remain staged follow-ups.
- CLIENT TRACKING / server-assisted client-side caching, stage 1 (issue #409):
  `CLIENT TRACKING ON|OFF [NOLOOP]` and `CLIENT TRACKINGINFO` (RESP3). A tracking
  connection's reads register their keys in the serving shard's tracking table;
  when another connection changes a tracked key, the tracking client receives a
  RESP3 `["invalidate", [key]]` push so it can drop its local cache, and
  `FLUSHALL`/`FLUSHDB` push the flush form (`["invalidate", nil]`, drop
  everything). `NOLOOP` suppresses the echo for a connection's own writes; an
  invalidation is one-shot (the client re-reads to be re-tracked). `ON` requires
  RESP3 (the `REDIRECT` target for RESP2 is a later stage); `OFF`/`RESET`/
  disconnect purge the connection from the table. The common no-tracking path is a
  single cheap gate per command (the table is never created until a tracking
  client reads), so a server with no tracking clients pays nothing. The
  `BCAST`/`OPTIN`/`OPTOUT`/`REDIRECT`/`PREFIX` options are rejected with a clear
  not-yet-supported error (staged follow-ups) rather than silently mis-moded, and
  this stage is single-shard-correct (the default deployment; cross-shard tracking
  is a documented follow-up).
- Observability command parity, part 1 (issue #413): `INFO COMMANDSTATS` and
  `INFO ERRORSTATS`. `INFO commandstats` (and `INFO all` / `everything`) now reports
  one `cmdstat_<cmd>:calls=N,usec=N,usec_per_call=N.NN,rejected_calls=N,failed_calls=N`
  line per command, and `INFO errorstats` one `errorstat_<CODE>:count=N` line per error
  reply code, matching the Redis field shapes go-redis / redis-py parse. The per-command
  tally is recorded on the serving shard off the already-encoded reply (no second
  dispatch); the timing read is shared with the SLOWLOG hook, and `CONFIG RESETSTAT`
  clears both tables. The default `INFO` omits these sections (Redis keeps the default
  reply small). `HOTKEYS` is the part-2 follow-up.
- Sharded Pub/Sub (issue #410): `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH` plus the
  `PUBSUB SHARDCHANNELS` / `PUBSUB SHARDNUMSUB` introspection (Redis 7.0). Shard
  channels live in a namespace SEPARATE from regular channels: an `SPUBLISH` delivers
  only to `SSUBSCRIBE` subscribers (a `smessage` push) and never to a `SUBSCRIBE`
  subscriber, and vice versa, with each command's receiver count and `PUBSUB`
  introspection scoped to its own namespace. A RESP2 subscriber may issue the shard
  (un)subscribe control commands in subscribe mode (not `SPUBLISH`, like `PUBLISH`),
  and the `ssubscribe`/`sunsubscribe` confirmation count is the shard-channel count
  only (independent of the channels+patterns total). Because IronCache Pub/Sub is
  node-local (no cross-node bus), an `SPUBLISH` is already confined to the node, which
  is the sharded "no cluster-bus fan-out amplification" property at the node boundary;
  the cluster-mode channel-slot to owner-node redirect is a documented follow-up. The
  commands are `@pubsub` and serve-routed (no scripting/keyspace change). The full
  cross-shard delivery + introspection reuses the existing per-shard subscription
  substrate.
- DEBUG conformance subcommand subset (issue #411): the `DEBUG` container, scoped
  strictly to what the upstream Redis/Valkey TCL conformance suites drive their
  assertions through, so those suites can run against IronCache unmodified (which
  strengthens the byte-for-byte differential claim far more than the command's
  end-user value). `DEBUG OBJECT <key>` reports a status line carrying the value's
  internal `encoding` (aligned with the OBJECT ENCODING mapping, so it cannot drift
  from `OBJECT ENCODING`), plus `refcount` / `serializedlength` / `lru` fields; a
  missing key is `-ERR no such key`. `DEBUG SET-ACTIVE-EXPIRE 0|1` toggles the
  node's background active-expiry cycle (the `0` state makes the active reaper inert
  so only lazy reap-on-access removes a key, the deterministic-expiry contract the
  suites rely on); the flag lives in the per-node runtime overlay so the toggle
  reaches every shard's drain. `DEBUG SLEEP <seconds>` blocks the serving shard then
  returns OK; `DEBUG STRINGMATCH-LEN <pattern> <string>` runs the same glob matcher
  KEYS/SCAN use and returns 1/0; `DEBUG JMAP` and `DEBUG QUICKLIST-PACKED-THRESHOLD`
  are accepted no-ops (IronCache has no JVM heap or quicklist packed nodes). DEBUG is
  ACL-gated under `@admin` + `@dangerous`; an unimplemented subcommand fails loudly
  (unknown subcommand), never a silent OK.
- Native compare-and-set and bulk-TTL primitives (issue #412): the atomic
  operations that previously forced a Lua script or a `WATCH`/`MULTI`/`EXEC` round
  trip, now as single server-side verbs (no scripting runtime, reinforcing the
  Tier-4 Lua non-goal). `SET` gains the Redis 8.4 compare-and-set options `IFEQ
  value` (write only if the current value equals `value`; a missing key is NOT
  created) and `IFNE value` (write only if it differs; a missing key IS created),
  mutually exclusive with `NX`/`XX` and composing with `GET` and the expiration
  options; the compare reads the value in place (no copy on the CAS hot path).
  `DELIFEQ key value` is the Valkey 9.0 compare-and-delete (the lock-release /
  leader-key pattern: delete only if the value is still our own token), returning
  `1` on a match, `0` otherwise, `WRONGTYPE` on a non-string. `MSETEX numkeys key
  value [...] [NX|XX] [EX|PX|EXAT|PXAT|KEEPTTL]` is the Redis 8.4 atomic multi-key
  set with a shared expiration (it extends `MSET`/`MSETNX`): the `NX`/`XX` gate is
  evaluated over every key before any write (all-or-nothing, reply `1`/`0`). The
  `MSETEX` key spec extracts exactly the strided keys after the `numkeys` prefix,
  so every key is gated through the ACL key-pattern check (the Redis 8.6 MSETEX
  ACL-bypass fix) with a restricted-user test. The digest-compare `SET` options
  `IFDEQ`/`IFDNE` are a documented follow-up (they need Redis's internal value
  digest, not reproduced here).
- Command-surface completeness (issue #414): `LOLWUT` now returns a non-error
  bulk string with the server version (so health probes and clients that call it
  succeed), and `BITOP` gains the Redis 8.2 set-algebra operators `DIFF`, `DIFF1`,
  `ANDOR`, and `ONE` alongside the existing `AND`/`OR`/`XOR`/`NOT`. `DIFF`/`DIFF1`/
  `ANDOR` combine the first source against the bitwise OR of the rest (and require
  at least two source keys); `ONE` keeps a bit set only where it appears in exactly
  one source. The combine is O(L*k) for a result of length L over k sources, the
  same lower bound Redis pays, and is shared byte-for-byte with the cross-shard
  spanning path.
- `ironcache upgrade` now performs a verified (sha256), data-safe (SAVE-first),
  health-gated, auto-rolling-back binary self-update (issue #387). The operator
  supplies a new binary with `--binary` and a release `--sha256sums`; the command
  verifies the binary's SHA-256 against its manifest entry, sanity-checks the new
  binary runs and reports its version (under a subprocess timeout, since the new
  binary is untrusted), triggers a synchronous fsync'd `SAVE` and confirms
  `LASTSAVE` advanced so the in-memory working set survives the restart (warning
  loudly and requiring `--yes` when no persistence is configured), pre-flights the
  `/readyz` health endpoint before touching disk, swaps the on-disk binary with a
  NEVER-ABSENT single-rename idiom (stage `.new`, hard-link/copy the current binary
  to the one retained `.old` rollback slot, then one atomic rename onto the target,
  so the `ExecStart` path is never momentarily missing and the live executable is
  never opened for write -- no ETXTBSY; a symlink target is refused, not clobbered),
  restarts the systemd unit, then health-gates the restarted server: it confirms
  the process actually RESTARTED and STABILIZED (the scraped
  `ironcache_uptime_seconds` reset below the pre-restart baseline and then crossed a
  5s stabilization window, so a no-op restart / stale process / crash-loop cannot
  pass), plus `/readyz` 200, `PING`/PONG, and the exact target version. On any
  health-gate miss it auto-rolls-back to the prior binary while PRESERVING the
  `.old` slot (so a subsequent failed upgrade can still roll back), unless
  `--no-rollback`. It is operator-run and privileged, never a RESP surface.
  OPERATOR NOTE: a non-interactive invocation (piped/cron, no TTY) MUST pass
  `--yes` -- with no TTY the confirm prompt reads as a decline and the upgrade
  aborts. The cryptographic signature anchor (#386), HTTPS/GitHub auto-fetch, and
  the lossless write-freeze (#388) are explicit follow-ups, with the `Verifier`,
  `BinarySource`, and `Saver` trait seams left in place for them. The packaged
  systemd unit's `ExecStart` now passes `--metrics-addr 127.0.0.1:9121` so the
  upgrade health gate's `/readyz` + `/metrics` endpoint is served.
- IronCache Console node-level MANAGEMENT layer (issue #361, single-node subset):
  the console becomes a bounded WRITE surface against one node, not just a monitor.
  New admin-tier write endpoints (`CONFIG SET`, key CRUD over `POST`/`DELETE
  /api/keys/{k}` plus expire/persist, an arbitrary-command Console at `POST
  /api/command`, pub/sub publish, ACL user management `SETUSER`/`DELUSER`, and a
  persistence `SAVE`/`BGSAVE`) plus the sensitive management reads (`CONFIG GET`,
  a `SCAN` key browser and key inspector, the pub/sub channel list, the ACL list,
  and the `INFO persistence` status). The HTTP responder, previously GET/HEAD-only,
  now reads a request body bounded by the existing request-size cap (an oversized
  body is a 413, malformed JSON a 400, never a panic) and routes POST/DELETE
  through the SAME tier gate that protects GET. Every mutation is ADMIN-tier,
  enforced server-side BEFORE the handler runs (a write verb maps to Admin
  regardless of path, so a trailing-slash / casing / method trick cannot drop it
  below the admin bar), and is audit-logged via tracing (action + target + result
  + the authenticated tier; never a value or secret). The console executes every
  command over its existing RESP connection AUTHed as the least-privilege node ACL
  user, so the node ACL is the ultimate bound (defense in depth) even for the
  arbitrary-command Console. The arbitrary command is bounded (a non-empty argv, a
  per-arg cap, and a total-bytes cap); the SCAN browser caps the page size and the
  pattern length and uses SCAN, never KEYS; the Bearer-header auth is CSRF-safe (no
  cookie). The dashboard Config, Keyspace, Console, Pub/Sub, ACL, and a new
  Persistence page are now FUNCTIONAL (read plus write) in the bespoke Butlr design
  language, replacing their gated empty-states; a mutation without an admin token
  surfaces "admin required" and reveals the sign-in rather than a raw error. The
  strict CSP (`default-src 'self'`, no inline style/script/handlers) and the
  XSS-safe posture (every server string via textContent, never innerHTML) are
  preserved: dynamic values go through CSSOM / classList and handlers via
  addEventListener. The genuinely-cluster-only views (Cluster, Replication,
  Shards) stay gated with their honest empty-states (no fabricated data). The
  OpenAPI document is updated to describe every new path, method, and schema.
  Typed-value writes are limited to a string SET in v1 (typed collection writes
  are a follow-up).

- IronCache Console: the cluster overview totals now include
  `commands_processed` and `connections_received`, summed across the reachable
  nodes from each node's INFO Stats counters, so the dashboard can derive a true
  throughput (operations per second) client-side by differencing the cumulative
  counter between polls. The OpenAPI document is updated to match. These are
  aggregate counts, so they stay in the OPEN tier with the other cluster totals.

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
