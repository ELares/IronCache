# Design: Admin and introspection command family (CLIENT LIST/INFO/KILL/PAUSE/NO-EVICT/NO-TOUCH, COMMAND DOCS/INFO/COUNT/GETKEYS)

Issue: #150. Decisions: ADR-0009 (behavioral equivalence, not bit-identical),
ADR-0002 (shared-nothing thread-per-core, which drives byte-faithful vs
synthesized fields). Related: #15 (PROTOCOL: Tier 0 handshake CLIENT subset and
the open RESET question this resolves), #86 (OBSERVABILITY: INFO/SLOWLOG/LATENCY
and the metric registry these commands draw on), #104 (AUTH: the default user
RESET deauthenticates to), #137 (ADMISSION: maxmemory-clients client eviction
that NO-EVICT exempts), #40 (OBJECT ENCODING/DEBUG OBJECT, a sibling
introspection surface, out of scope here).

## Goal and scope

#15 scoped Tier 0 to the handshake only: it requires the `CLIENT`
subcommands a client touches on connect (`SETNAME/GETNAME/SETINFO/ID`) plus
`COMMAND DOCS`/`COMMAND COUNT` stubs, and explicitly deferred the admin
subcommands (`LIST`, `INFO`, `KILL`, `NO-EVICT`, `NO-TOUCH`) and the full
`COMMAND` introspection family to this audit-filed design. This document owns that
deferred surface: the operator-facing `CLIENT` administration commands and the
`COMMAND` introspection family that client libraries use for arity validation and
tooling. The contract is ADR-0009 behavioral equivalence against the pinned Valkey
oracle, not bit-identical replies; the load-bearing decision is which per-client
fields are byte-faithful and which are synthesized to a Redis-recognized shape
under the thread-per-core model (ADR-0002). It also resolves the RESET semantics
#15 left open.

## Design

### CLIENT LIST / INFO field fidelity (byte-faithful vs synthesized)

- `CLIENT LIST` returns one line per connection and `CLIENT INFO` returns the
  same line for the calling connection; both use the Redis per-line field set
  (`id addr laddr fd name age idle flags db sub psub ssub multi watch qbuf
  qbuf-free argv-mem multi-mem obl oll omem tot-mem events cmd user redir resp
  rbs rbp lib-name lib-ver tot-net-in tot-net-out tot-cmds`)
  [redis-client-list-fields], so an operator pasting Redis tooling or eyeballing
  the output sees the field names it expects (ADR-0009).
- Each field is classified as **byte-faithful** (a true value IronCache holds) or
  **synthesized** (emitted to a recognized shape because the thread-per-core model
  has no identical internal). Byte-faithful: `id`, `addr`, `laddr`, `fd`, `name`,
  `age`, `idle`, `db`, `flags`, `sub`/`psub`/`ssub`, `multi`/`watch`, `cmd`,
  `user`, `resp`, `redir`, `lib-name`, `lib-ver`, `tot-net-in`/`tot-net-out`,
  `tot-cmds`: all are shard-core-local per-connection state IronCache already
  tracks (PROTOCOL #15 per-connection state, the read/write byte counters
  feeding #86). `lib-name`/`lib-ver` come straight from `CLIENT SETINFO`, and the
  network byte counters are first-class (the same source as the observability
  counters, #86). Synthesized: the buffer-internal fields (`qbuf`, `qbuf-free`,
  `argv-mem`, `multi-mem`, `obl`, `oll`, `omem`, `rbs`, `rbp`, `tot-mem`) are
  reported from IronCache's own per-core buffer accounting (ADR-0002 buffers are
  per-core, not the Redis single-thread reply list), mapped onto the Redis field
  so a parser reads a sane number even though the underlying structure differs;
  `io-thread` has no IronCache analog (every connection is pinned to its owning
  core, ADR-0002) and reports that core's id. The doc enumerates each field's
  class so the conformance oracle (#96/#97) knows which fields to diff for value
  and which to diff for shape only.
- `CLIENT LIST` accepts the `TYPE NORMAL|MASTER|REPLICA|PUBSUB` filter and the
  `ID id [id ...]` filter [redis-client-list-fields]; gathering the list walks
  every core's connection table by message-passing (ADR-0002, no shared client
  registry), assembled on the issuing core, so the command is O(N) over
  connections and off the GET/SET fast path.

### CLIENT KILL filter forms

- Both `CLIENT KILL` forms are supported: the legacy `CLIENT KILL <addr:port>`
  (returns `OK`, or an error if no such client) and the filter form
  `CLIENT KILL <filter> <value> ...` combining `ID`, `ADDR`, `LADDR`,
  `TYPE normal|master|replica|pubsub`, `USER username`, `SKIPME yes|no`
  (default `yes`, so the caller is spared unless `SKIPME no`), and `MAXAGE`
  (kill connections older than N seconds); the filter form returns the integer
  count killed [redis-client-kill-filters]. Filters combine with logical AND.
- A kill targeting a connection owned by another core is an explicit cross-core
  message to that core's reactor (ADR-0002), which closes the connection on its
  owning core; there is no cross-core mutation of connection state. The integer
  count is the sum of per-core acknowledgements.

### CLIENT PAUSE / UNPAUSE (WRITE | ALL)

- `CLIENT PAUSE timeout [WRITE | ALL]` suspends command processing for `timeout`
  milliseconds; `ALL` (the default when no mode is given) holds all client
  commands, while `WRITE` holds only writes and keeps serving reads
  [redis-client-pause-modes]. `CLIENT UNPAUSE` lifts an active pause early. The
  `PAUSE` command itself replies `OK` immediately and is not itself paused.
- Pause is a per-core flag set on every shard core via a broadcast message
  (ADR-0002): each core stops draining its own paused connections, so the pause
  is shard-local enforcement of a process-wide intent, with no shared pause lock.
  `WRITE` mode keys off the command's write flag (the same flag surfaced by
  `COMMAND INFO` below), so reads and memory-releasing commands continue, matching
  the failover use case (pause writes, let replicas catch up, promote, unpause).

### CLIENT NO-EVICT and NO-TOUCH

- `CLIENT NO-EVICT ON|OFF` exempts the calling connection from the
  `maxmemory-clients` client-eviction mechanism (#137): when `ON`, the
  connection's query/output buffers are not counted as an eviction candidate, so
  a critical admin or replica connection is not dropped under client-buffer
  pressure [redis-client-no-evict-no-touch]. This binds directly to the ADMISSION
  aggregate client-buffer cap (#137): the per-core buffer accounting skips a
  `NO-EVICT` connection when choosing the largest-buffer victim.
- `CLIENT NO-TOUCH ON|OFF` stops commands on this connection from updating keys'
  LRU/LFU access metadata, so an admin or scanning connection can read without
  perturbing eviction statistics [redis-client-no-evict-no-touch]; `OFF` restores
  normal touch. This binds to the eviction metadata (ADR-0008 default S3-FIFO and
  the EvictionPolicy trait): the per-connection no-touch flag suppresses the
  access-time/frequency bump on the read path for that connection only.

### COMMAND DOCS / INFO / COUNT / GETKEYS

- The `COMMAND` introspection family is specified to its Redis reply shapes so
  client libraries that fetch the command table at startup validate arity
  unchanged [redis-command-introspection]: `COMMAND INFO` returns an array with
  one nested entry per command of `name, arity, flags, first-key, last-key, step`
  plus the post-6.0 `acl-categories, tips, key-specs, subcommands`; `COMMAND
  COUNT` returns an integer; `COMMAND GETKEYS` returns the array of keys a given
  full invocation would touch; `COMMAND DOCS` returns a map of command name to its
  documentation. A non-existent command yields `nil` in its `COMMAND INFO` slot.
- The table is generated from one in-binary command-spec registry (the per-command
  semantics owned by #128/#129): arity, flags, and the key-spec
  (first-key/last-key/step) are the single source `COMMAND INFO`, `COMMAND
  GETKEYS`, and the dispatcher's own arity check all read, so client-side arity
  validation matches server-side enforcement by construction. `COMMAND GETKEYS`
  computes keys from the same key-spec, which is also what the cluster slot router
  (CLUSTER_CONTRACT #70) uses, so key extraction is consistent across the
  introspection and routing paths. `COMMAND DOCS` ships a real table for the
  supported tiers (ADR-0009) and omits unsupported commands rather than faking
  them, keeping the introspection surface honest with the compatibility map. This
  supersedes the Tier 0 `COMMAND DOCS`/`COUNT` startup stubs PROTOCOL.md (#15)
  carried.

### RESET semantics (resolving the #15 open question)

- PROTOCOL.md (#15) left exact `RESET` semantics open pending this design. RESET
  clears the per-connection state enumerated in PROTOCOL.md back to a
  freshly-connected connection: it discards any `MULTI` queue and `WATCH` set
  (aborting a transaction), resets the protocol to RESP2, deauthenticates to the
  default user (AUTH.md #104 default user, ADR-0009) [acl-default-user],
  re-`SELECT`s DB 0, clears the connection name, turns off `MONITOR` and
  client-side-caching tracking, and disables `NO-EVICT`/`NO-TOUCH` and any
  per-connection reply mode. It does not close the connection and does not touch
  keyspace data. All of this is shard-core-local state on the owning core
  (ADR-0002), so RESET is a local state reset with no cross-core effect. RESET
  replies `+RESET`.

## Open questions

- Whether the synthesized buffer fields (`qbuf`/`rbs`/`obl`/`omem`/`tot-mem`)
  should report IronCache's true per-core buffer bytes or a Redis-shaped
  approximation when the per-core model has no one-to-one mapping; the conformance
  oracle (#97) decides how tightly these are diffed (value vs shape only).
- Whether `CLIENT NO-EVICT ON` should also imply a higher output-buffer hard
  limit for that connection (#137) or only exempt it from the aggregate cap.
- The exact `io-thread` value semantics under thread-per-core (report the owning
  core id vs a fixed sentinel), decided against the field's only real consumer.

## Acceptance and test hooks

- `CLIENT LIST`/`CLIENT INFO` emit every Redis field name in the pinned order; the
  differential oracle (#96/#97) diffs byte-faithful fields for value and
  synthesized fields for shape, against pinned Valkey [redis-client-list-fields].
- `CLIENT KILL` legacy form returns `OK`/error and the filter form returns the
  integer count; an `ADDR`+`TYPE` combined filter kills only matching clients;
  `SKIPME no` kills the caller (a cross-core kill test) [redis-client-kill-filters].
- `CLIENT PAUSE timeout WRITE` blocks writes while `GET` still succeeds, `ALL`
  blocks both, and `CLIENT UNPAUSE` lifts the pause early (a pause-mode test)
  [redis-client-pause-modes].
- `CLIENT NO-EVICT ON` keeps the connection alive under a client-buffer flood
  that evicts non-exempt clients (#137); `CLIENT NO-TOUCH ON` leaves a hot key's
  LRU/LFU metadata unchanged across a read (an eviction-metadata test)
  [redis-client-no-evict-no-touch].
- `COMMAND COUNT` returns an integer, `COMMAND INFO GET SET MISSING` returns the
  10-element nested arrays with a `nil` for the missing command, `COMMAND GETKEYS
  SET k v` returns `[k]`, and `COMMAND DOCS` returns a map; an unmodified client
  that validates arity from `COMMAND INFO` connects and runs unchanged (a
  client-startup test) [redis-command-introspection].
- `RESET` returns `+RESET` and clears MULTI/WATCH, proto, auth, name, DB,
  tracking, MONITOR, and NO-EVICT/NO-TOUCH to defaults without closing the
  connection (a RESET state-clear test), confirming the #15 resolution.

## References

- ADR-0002 (shared-nothing thread-per-core; byte-faithful vs synthesized fields,
  per-core buffers, cross-core kill/pause messaging), ADR-0009 (behavioral
  equivalence, the compatibility map COMMAND DOCS honors), ADR-0008 (default
  eviction policy S3-FIFO and the eviction metadata NO-TOUCH suppresses);
  AUTH.md (#104, the default user RESET deauthenticates to); issues #15
  (PROTOCOL, deferred CLIENT subset and the RESET open question), #86
  (OBSERVABILITY, INFO and the byte counters), #137 (ADMISSION,
  maxmemory-clients NO-EVICT exempts), #128/#129 (per-command spec registry
  feeding COMMAND), #70 (CLUSTER_CONTRACT, shared key-spec), #40 (sibling
  OBJECT/DEBUG introspection, out of scope), #96/#97 (conformance/differential
  oracle).
- Claims: [redis-client-list-fields], [redis-client-kill-filters],
  [redis-client-pause-modes], [redis-client-no-evict-no-touch],
  [redis-command-introspection], [acl-default-user].
