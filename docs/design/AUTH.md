# Design: AUTH handshake and credential model

Issue: #104 (spun out of #22). Decisions: ADR-0009 (compat tiering, AUTH is a
Tier 0 connection command), ADR-0002 (shared-nothing, auth state lives per
connection off the data path). Related: #15/PROTOCOL.md (HELLO state machine),
#18/ERRORS.md (error catalog), #106/ACL.md (full ACL, deferred), #105/TLS.md
(transport, separate spec).

## Goal and scope

Every mainstream client negotiates authentication through the connection
handshake, so if IronCache gets the AUTH contract wrong, clients fail to connect
at all regardless of engine speed. This specifies the credential model and the
`HELLO AUTH` / `AUTH` / `requirepass` surface, the SHA-256 password storage, and
the minimal default-user shape, layered onto the per-connection state machine so
it does not tax GET/SET. In scope: the authentication handshake and the
default-user. Out of scope (specified elsewhere): the full `@category`/`%R%W`/
selector ACL engine and `aclfile` (#106), and the TLS transport (#105).

## Design

### Default user and credential model

- The default user ships in the Redis-compatible shape `user default on nopass
  ~* &* +@all` [acl-default-user], so an unconfigured IronCache accepts commands
  with no auth, matching Redis's documented default-user (behavioral equivalence,
  ADR-0009). `ACL GETUSER default` must report the field set a Redis client
  expects; that field-by-field output is a design requirement verified
  differentially against the oracle, not asserted here. Passwords are stored as
  SHA-256 [acl-default-user]; IronCache keeps SHA-256 rather than a stronger KDF
  as a deliberate behavioral-equivalence choice (ADR-0009) so the stored form
  matches Redis, with the accepted risk recorded in the threat model (#142).

### requirepass maps onto the default user

- Legacy `requirepass <pass>` sets the default user's password [acl-default-user],
  flipping it from `nopass` to password-required, rather than living as a separate
  parallel code path. The #22 open decision is resolved by this design:
  `requirepass`, `AUTH default <pass>`, and `ACL SETUSER default >pass` all
  converge on the same default-user credential, so the three configuration routes
  cannot disagree about the effective password.

### Handshake surface: HELLO AUTH and AUTH

- `AUTH <pass>` (one-arg, legacy) authenticates as the default user; `AUTH <user>
  <pass>` authenticates as a named user; `HELLO <ver> AUTH <user> <pass>` does the
  same inline during protocol negotiation. Modern clients default to RESP3 and so
  open with `HELLO 3` (redis-py 8, node-redis 6 [client-default-resp3-redis8]);
  carrying credentials inline on that `HELLO` is the general handshake behavior
  IronCache must accept.
  AUTH and the AUTH arguments to HELLO run inside the Tier 0 handshake
  (PROTOCOL.md) and gate the connection before any data command is dispatched.
- Authentication state is a per-connection flag resolved at handshake time and
  read with a single branch on the data path (ADR-0002): a connection is either
  authenticated-as-user-U or not, with no shared lookup per command.

### Error contract (referenced, not redefined)

- Failures use the exact prefixes already catalogued in ERRORS.md: `-NOAUTH` when
  auth is required and absent, `-WRONGPASS` on a bad username/password, and
  `-NOPROTO` on an unsupported `HELLO` version [hello-noproto-error]. This spec
  references that catalog rather than restating the byte strings, so there is one
  source of truth for wording.

### Differential conformance

- The handshake is verified against a pinned Valkey (BSD-3 [valkey-license-bsd3],
  RESP-wire-compatible [valkey-resp-identical]) as the oracle (TESTING.md): the
  same `HELLO`/`AUTH`/`requirepass` sequences are replayed against both and the
  wire replies diffed, so an auth divergence surfaces as a failing differential
  case.

## Open questions

- Whether AUTH attempt-rate limiting (throttling repeated `-WRONGPASS`) is in M1
  or deferred with the full ACL (#106); the threat model (#142) informs this.
- Whether `HELLO` without AUTH on a password-required server returns the partial
  pre-auth map or `-NOAUTH` (match the pinned oracle exactly, #97).

## Acceptance and test hooks

- `HELLO 3 AUTH default <pass>`, `AUTH <pass>`, `AUTH <user> <pass>`, and
  `requirepass` all authenticate unmodified redis-cli/redis-py/ioredis clients.
- Bad credentials return `-NOAUTH`/`-WRONGPASS` and an unsupported HELLO version
  returns `-NOPROTO`; these prefixes are in ERRORS.md's full-text-pinned set, so
  the wording matches verbatim [hello-noproto-error], not just the leading token.
- Passwords are stored as SHA-256 [acl-default-user]; `ACL GETUSER default` output
  matches the oracle differentially; `requirepass`, `AUTH default`, and `ACL
  SETUSER default >pass` converge on the same effective credential.
- A single-key GET/SET on an authenticated connection takes one branch for the
  auth check and no shared lookup (the hot-path lint, ADR-0002).

## References

- ADR-0002, ADR-0009; issues #22, #15, #18, #105, #106, #142, #97, #1;
  specs PROTOCOL.md, ERRORS.md, TESTING.md.
- Claims: [acl-default-user], [hello-noproto-error], [client-default-resp3-redis8],
  [valkey-license-bsd3], [valkey-resp-identical].
