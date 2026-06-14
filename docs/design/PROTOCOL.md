# Design: RESP protocol surface, parser, and connection state machine

Issue: #15. Decisions: ADR-0009 (compatibility tiering), ADR-0019 (reply
shaping). Related: #18 (error catalog), #138 (hardening), #95 (conformance).

## Goal and scope

Keeping the Redis contract is IronCache's headline promise, and it decomposes
into three surfaces versioned separately: the wire serialization (RESP2/RESP3
framing), the interaction model (pipelining, inline commands, the per-connection
state machine, push), and the command surface (owned by #128/#129). This
document specifies the bottom of the stack: the parser, the serializer, the
`HELLO`-driven per-connection state, and the Tier 0 connection commands every
unmodified client touches on connect, so redis-cli, redis-py, ioredis, Jedis,
go-redis, and StackExchange.Redis connect without modification. All conflicts
resolve to Compatible first.

## Design

### Wire parser

- A single decoder handles both RESP2 multibulk requests and inline commands.
  Requests are arrays of bulk strings; the decoder reads `*<n>` then `<n>`
  `$<len>\r\n<bytes>` items. Inline commands (a bare line with no `*`) are
  accepted for redis-cli and netcat ergonomics and split on whitespace.
- The parser is incremental and non-blocking: it consumes from a per-connection
  read buffer owned by the connection's core (shared-nothing, ADR-0002), returns
  one fully-parsed command at a time, and never copies the payload where a borrow
  into the read buffer suffices (zero-copy into the command dispatch; the value
  is copied only when it is stored). SIMD scanning of CRLF and lengths is a later
  optimization behind the same interface, not part of this contract.
- All size and shape limits (bulk length, multibulk count, nesting, inline
  length, accumulated incomplete-frame bytes, parser-work budget) are enforced
  here per the hardening design (#138); the 512 MB bulk cap is the default of the
  tunable `proto-max-bulk-len` [bulk-string-max-512mb], not a hard constant.

### Serializer and reply shaping

- The serializer is parameterized by the connection's negotiated protocol. Under
  RESP2 it emits RESP2 shapes; under RESP3 it emits native aggregates (map `%`,
  set `~`, double `,`, big number `(`, verbatim `=`, push `>`) exactly where
  Redis does, per ADR-0019 and the type markers [resp-type-prefixes]. Null
  follows ADR-0019 (`_` under proto=3, `$-1`/`*-1` under proto=2)
  [resp2-null-encodings].
- Error replies come only from the canonical catalog (#18); the serializer never
  hand-writes error text.

### Per-connection state machine (HELLO-driven)

- A connection starts in RESP2 [resp3-opt-in-via-hello]. `HELLO` with no version
  reports server info and keeps the current proto; `HELLO 2` / `HELLO 3` switch
  proto; an unsupported version returns `-NOPROTO` [hello-noproto-error].
- Per-connection state: protocol version, authenticated user (default user per
  [acl-default-user]), selected DB, name, RESP3 push/tracking flags, and the
  MULTI queue (the transaction surface is ADR-0010). State is shard-core-local;
  no cross-core sharing.

### Tier 0 connection commands (ADR-0009)

`PING`, `HELLO`, `AUTH`, `SELECT`, `QUIT`, `RESET`, and the `CLIENT`
subcommands needed so handshakes succeed (`CLIENT SETNAME/GETNAME/SETINFO/ID`),
plus `COMMAND DOCS`/`COMMAND COUNT` stubs sufficient for client startup. The full
admin `CLIENT`/`COMMAND` surface is a later design; Tier 0 only needs the
handshake to complete for every mainstream client against the ~240-command
expectation [redis-core-command-count].

### Interaction model

- Pipelining: the decoder yields commands as fast as they arrive; replies are
  written in request order. No request reordering.
- Push: RESP3 push frames (`>`) carry pub/sub [sharded-pubsub-7.0] and
  client-side-caching invalidation [client-tracking-options]; under RESP2,
  invalidation uses the `__redis__:invalidate` channel [resp2-invalidation-channel].
  The push delivery mechanism is detailed with the server-push design (#20).

## Open questions

- Exact `RESET` semantics (what connection state it clears) pending the admin
  command design.
- Whether `COMMAND DOCS` ships a real table in v1 or a minimal stub (driven by
  which clients hard-require it at startup).

## Acceptance and test hooks

- Every mainstream client (redis-cli, redis-py, ioredis, go-redis, Jedis,
  StackExchange.Redis) completes connect + `HELLO` + a `GET`/`SET` round trip
  unmodified, under both proto=2 and post-`HELLO 3`.
- The differential oracle (#96/#97) diffs framing and reply shapes against pinned
  Valkey [valkey-resp-identical] in both proto modes.
- Parser fuzzing and the hardening limits (#138) are merge-gating for this crate.

## References

- ADR-0009 (tiers), ADR-0019 (reply shaping); issues #18, #138, #20, #128, #129,
  #95, #96.
- Claims: [resp3-opt-in-via-hello], [resp-type-prefixes], [resp2-null-encodings],
  [hello-noproto-error], [bulk-string-max-512mb], [acl-default-user],
  [client-default-resp3-redis8], [redis-core-command-count], [sharded-pubsub-7.0],
  [client-tracking-options], [resp2-invalidation-channel], [valkey-resp-identical].
