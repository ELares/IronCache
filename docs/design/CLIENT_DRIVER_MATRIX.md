# Design: Real client-driver compatibility matrix

Issue: #158. Decisions: ADR-0009 (behavioral-equivalence tiering), ADR-0019
(RESP3 matched per command, RESP2 fallback), ADR-0003 (determinism / Env seam,
for reproducible reconnect and timeout points). Related: #95 (correctness stack,
parent, TESTING.md), #97 (differential replay vs pinned Valkey/Redis,
DIFFERENTIAL_TESTING.md), #18 (error catalog, ERRORS.md, the leading-token bar
and exact-text exception set drivers pattern-match), #150 (ADMIN_COMMANDS.md, the
RESET and CLIENT LIST/INFO contract these suites exercise), #20 (SERVER_PUSH.md,
the RESP3 push frame), #21 (CLIENT_TRACKING.md, the invalidation push these
clients consume), #15 (PROTOCOL.md, HELLO and the per-connection state machine),
#96 (the pinned-oracle and version-pin discipline this matrix reuses), #1 (vision
EPIC, Compatible is the top tenet).

## Goal and scope

Compatible is the top tenet, and the only proof that a real client works against
IronCache is to run that client's own integration suite, the suite its maintainers
gate their own releases on, against IronCache. The byte-diff harness (#97) proves
IronCache matches Valkey frame for frame, but no real driver is ever in its loop,
so it cannot catch the breakages that live in the driver's own assumptions:
connection-pool checkout and health-check, request pipelining and out-of-order
guards, reconnect and `RESET` handling, `CLIENT INFO`/`CLIENT LIST` line parsing,
RESP3 push-frame dispatch, and error-string pattern matching. This spec defines a
harness that stands up an IronCache instance and points each major driver's
unmodified upstream test suite at it, across a pinned per-driver version matrix,
once on the RESP2 default and once after `HELLO 3` RESP3 [resp3-opt-in-via-hello],
and records each suite as merge-gating or as a tracked exception with a written
reason. Scope: single-node, the seven mainstream drivers below, their own suites
plus a thin smoke app per driver. Out of scope: cluster-mode client behavior
(MOVED/ASK follow, deferred to the clustering tests), throughput (#8), and the
frame-level conformance the differential suite (#97) already owns. This matrix
consumes #97's pinned-oracle and version-pin discipline and reuses ERRORS.md (#18)
as the error-string contract; it does not re-derive them.

## Design

### Why driver suites catch what byte-diff cannot

- The differential suite (#97) replays a command stream IronCache and Valkey both
  parse the same way and diffs the reply bytes; it is the authority on per-frame
  fidelity. It is blind to anything the driver does between frames: how a pool
  decides a connection is dead and reconnects, how a pipeline multiplexer maps
  replies back to in-flight requests, how a client parses the free-form
  `CLIENT INFO` line, and how it dispatches an unsolicited RESP3 push. Those paths
  only execute when the real driver is the thing under test. This matrix is
  therefore complementary to #97, not a duplicate: #97 proves the bytes are right,
  this proves real drivers behave on top of those bytes.

### The driver set and the pinned version matrix

- Seven mainstream drivers, each run at a pinned set of versions rather than
  "latest", so a failure is reproducible and a green run is attributable to an
  exact client build (the same version-pin rule #95/#96 apply to the Valkey
  oracle):
  - redis-py (Python), node-redis and ioredis (Node), Jedis and Lettuce (JVM),
    go-redis (Go), and StackExchange.Redis (.NET).
- Each driver pins at least two points: its current major and the prior major
  still in wide use, because the default wire protocol changed across that
  boundary for several of them. Modern official clients now default to RESP3 on
  the wire while the server still starts in RESP2: redis-py 8.0 and node-redis 6.0
  send `HELLO 3` by default [client-default-resp3-redis8], and go-redis, Jedis,
  and Lettuce default to RESP3 in their current generation
  [go-redis-jedis-lettuce-default-resp3]. StackExchange.Redis is the deliberate
  outlier: it defaults to RESP2 and treats RESP3 as opt-in
  [stackexchange-redis-default-resp2-optin], so its matrix must force RESP3 on to
  exercise the upgrade path the others take by default. Running each driver at
  both its RESP3-default and a pinned RESP2 point is how the matrix covers both
  protocols without depending on any one client's default.
- The pinned versions live in a committed `CLIENT_VERSIONS` table, bumped only by
  explicit PR, mirroring the `VALKEY_VERSIONS` discipline in DIFFERENTIAL_TESTING.md
  (#97). A client major bump is a reviewed change, not a floating tag, so a new
  driver release that breaks against IronCache surfaces as a failing PR rather than
  as silent CI drift.

### Run each suite as-is, double-run over both protocols

- The harness runs each driver's own upstream integration suite unmodified
  [redis-drivers-ship-server-integration-test-suites] against a freshly booted
  zero-config IronCache, the same default posture (eviction and a memory ceiling
  on) a real install has. "Unmodified" is the point: the moment we fork a suite to
  make it pass we stop testing the client and start testing our fork of it. The
  only sanctioned modification is the connection target (host/port and, where the
  driver does not already, the forced protocol).
- Every suite is run twice, once on the RESP2 default and once with the driver
  configured to send `HELLO 3` [resp3-opt-in-via-hello], because null and
  aggregate reply shapes differ by protocol [resp2-null-encodings] and the per-
  command RESP3 shape IronCache emits is matched to Redis per ADR-0019. For the
  five RESP3-default drivers the RESP2 run is the explicit-downgrade point; for
  StackExchange.Redis the RESP3 run is the explicit-upgrade point
  [stackexchange-redis-default-resp2-optin]. A suite that passes on one protocol
  and fails on the other is a per-protocol exception, recorded as such.

### Coverage axes the suites must exercise

- The matrix is only meaningful if the chosen suites actually touch the paths the
  byte-diff harness cannot. Each driver entry records which axes its suite covers,
  and a gap on a load-bearing axis is itself tracked:
  - Connection pooling: pool checkout/return, max-pool saturation, idle reaping,
    and the per-connection health check (often a `PING` or a `HELLO`/`CLIENT
    SETINFO` on acquire) all complete against IronCache.
  - Pipelining: a batch of queued commands returns replies in submission order
    with no cross-talk, including a batch that mixes a failing command with
    succeeding ones (the driver must map the error to the right slot).
  - Reconnect and RESET: the client recovers a dropped connection and re-runs its
    handshake; `RESET` returns `+RESET` and leaves the connection usable, clearing
    MULTI/WATCH, protocol, auth, name, DB, and tracking back to defaults per
    ADMIN_COMMANDS.md (#150). Drivers that send `RESET` on pool return depend on
    this exact reply.
  - CLIENT INFO / CLIENT LIST: the driver parses the `CLIENT INFO` line and any
    `CLIENT LIST` output it relies on (some set and then read back a library
    name/version via `CLIENT SETINFO`); the field names and pinned order from
    ADMIN_COMMANDS.md (#150) must satisfy the driver's parser.
  - RESP3 push: on a RESP3 connection the driver correctly dispatches an
    unsolicited push frame (`>`) for Pub/Sub message, sharded Pub/Sub, keyspace
    notification, and client-side-caching invalidation, distinguishing it from a
    command reply, per SERVER_PUSH.md (#20) and CLIENT_TRACKING.md (#21).
  - Error strings: assertions in the suite that match on an error's leading token
    (`ERR`, `WRONGTYPE`, `NOPROTO`, `NOAUTH`, `EXECABORT`) and, where a client
    pattern-matches full wording, the exact text, all satisfied by ERRORS.md
    (#18). `NOPROTO` is fixed wording [hello-noproto-error] and the
    no-rollback EXECABORT semantics [multi-exec-no-rollback] are part of what
    transaction-aware suites assert.

### Merge-gating vs tracked-exception policy

- Each (driver, version, protocol) cell has one of three states. **Gating**: the
  cell is green and a regression fails the PR. **Tracked exception**: a named,
  individually skipped test (or whole suite on one protocol) with a written reason
  and a linked upstream-or-IronCache issue; the rest of that cell stays gating, so
  one quarantined test cannot mask the others. **Unsupported**: a cell the matrix
  deliberately does not run yet (for example a cluster-only suite), declared, not
  silently absent. The exception list is the only place a real-client failure is
  allowed to land, and every entry is reviewed in the PR that adds it, the same
  rule DIFFERENTIAL_TESTING.md (#97) applies to its byte-exact exemptions. A
  failure with no exception entry is a hard merge stop. A tracked exception that
  starts passing is removed (promoted back to gating) in the PR that observes it.

### Triage path when a suite fails

- A failing driver assertion is first reproduced against the pinned Valkey oracle
  with the same driver version. If Valkey also fails the assertion, the test is a
  driver/Valkey-version mismatch, not an IronCache bug, and the cell is pinned to a
  driver version that agrees with the oracle. If Valkey passes and IronCache
  fails, the divergence is a real IronCache compatibility bug and is reduced to a
  minimal command stream handed to the differential corpus (#97), so the cheap
  byte-diff gate gains a permanent regression case for what the expensive
  driver-suite run found. This keeps the slow real-client matrix as the discovery
  tool and the fast differential suite as the day-to-day guard.

### CI cadence (the suites are heavy)

- Running seven driver toolchains times their full suites times two protocols is
  far too heavy for every PR, so cadence is tiered. A thin per-driver smoke app
  (connect, `HELLO`, a handful of GET/SET/pipeline/Pub-Sub calls, a `RESET`) runs
  on every PR as a fast required check, because a smoke break means the wire
  contract moved. The full matrix runs on a nightly schedule and on any PR that
  touches the protocol, connection, command-dispatch, or error-catalog surface (a
  path filter), and the result feeds the published compatibility tier. A client
  version bump in `CLIENT_VERSIONS` always triggers a full matrix run. This keeps
  the per-PR cost bounded while guaranteeing the full proof runs before any
  compatibility-tier claim ships.

## Open questions

- Per-driver toolchain provisioning in CI (seven language runtimes plus the right
  build tools) and whether each runs in its own pinned container image versioned
  alongside `CLIENT_VERSIONS`.
- How deep the real-app layer goes beyond the smoke app: whether to add one or two
  representative real frameworks per ecosystem (for example a cache-aside ORM
  layer) or to rely on the drivers' own suites for application-level coverage.
- How to treat a suite that hard-requires a feature IronCache declares a non-goal
  (Lua scripting on the hot path, per #1): skip with a documented non-goal reason
  vs maintain a curated subset, decided per driver.
- Whether StackExchange.Redis's RESP2-default cell stays gating on RESP2 only or
  also gates its forced-RESP3 cell from day one, given its opt-in posture
  [stackexchange-redis-default-resp2-optin].
- Flaky-test policy for upstream suites that are themselves timing-sensitive
  (reconnect/idle-reap tests), and the retry budget before a cell is quarantined.

## Acceptance and test hooks

- Each of the seven drivers (redis-py, node-redis, ioredis, Jedis, Lettuce,
  go-redis, StackExchange.Redis) runs its own unmodified upstream integration
  suite [redis-drivers-ship-server-integration-test-suites] against a zero-config
  IronCache, at the versions pinned in `CLIENT_VERSIONS`.
- Every suite runs under both protocols: the RESP2 default and post-`HELLO 3`
  RESP3 [resp3-opt-in-via-hello], with the RESP3-default drivers
  [client-default-resp3-redis8][go-redis-jedis-lettuce-default-resp3] also
  exercised on a forced-RESP2 point and StackExchange.Redis
  [stackexchange-redis-default-resp2-optin] also on a forced-RESP3 point;
  null/aggregate shapes are correct per protocol [resp2-null-encodings].
- Coverage is asserted on every axis: a pool saturation+reaping test, a
  pipelined batch that preserves reply order across a mixed success/error batch, a
  drop-and-reconnect test, a `RESET`-returns-`+RESET`-and-clears-state test (#150),
  a `CLIENT INFO` parse test (#150), a RESP3 push-dispatch test for Pub/Sub and
  client-side-caching invalidation (#20, #21), and an error-token assertion test
  whose strings come from ERRORS.md (#18) [hello-noproto-error][multi-exec-no-rollback].
- The merge-gating vs tracked-exception policy is enforced: a failing cell with no
  reviewed exception entry hard-fails the PR; each exception carries a written
  reason and a linked issue; an exception that starts passing is promoted back to
  gating in the same PR that observes it.
- A real-client failure that the pinned Valkey oracle passes is reduced to a
  minimal command stream and added to the differential corpus (#97), so the fast
  byte-diff gate gains a permanent regression for it.
- Cadence holds: a per-driver smoke app is a required per-PR check; the full
  matrix runs nightly, on protocol/connection/dispatch/error-surface path
  changes, and on any `CLIENT_VERSIONS` bump, and gates the published
  compatibility tier.

## References

- ADR-0003, ADR-0009, ADR-0019; issues #95, #97, #18, #150, #20, #21, #15, #96,
  #1 (vision); specs TESTING.md, DIFFERENTIAL_TESTING.md, ERRORS.md,
  ADMIN_COMMANDS.md, SERVER_PUSH.md, CLIENT_TRACKING.md, PROTOCOL.md.
- Claims: [redis-drivers-ship-server-integration-test-suites],
  [go-redis-jedis-lettuce-default-resp3], [stackexchange-redis-default-resp2-optin],
  [client-default-resp3-redis8], [resp3-opt-in-via-hello], [resp2-null-encodings],
  [hello-noproto-error], [multi-exec-no-rollback].
