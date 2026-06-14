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
  with no auth (the cache-mode posture, ADR-0007) and `ACL GETUSER default`
  reports the expected fields. Passwords are stored and reported as SHA-256
  [acl-default-user], not a stronger KDF: SHA-256 is the contract `ACL GETUSER`
  output and existing tooling expect, and diverging would break compatible output
  (Compatible outranks any marginal hardening here; the threat model #142 records
  the accepted risk).

### requirepass maps onto the default user

- Legacy `requirepass <pass>` sets the default user's password and flips it from
  `nopass` to password-required [acl-default-user]; it is not a separate parallel
  code path. This resolves the #22 open decision: `requirepass`, `AUTH default
  <pass>`, and `ACL SETUSER default >pass` all converge on the same default-user
  credential, so the three configuration routes cannot disagree about the
  effective password.

### Handshake surface: HELLO AUTH and AUTH

- `AUTH <pass>` (one-arg, legacy) authenticates as the default user; `AUTH <user>
  <pass>` authenticates as a named user; `HELLO <ver> AUTH <user> <pass>` does the
  same inline during protocol negotiation, which is how RESP3-default clients
  (redis-py 8, node-redis 6 [client-default-resp3-redis8]) carry credentials.
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
  returns `-NOPROTO`, byte-matching the pinned oracle [hello-noproto-error].
- Passwords are stored and reported as SHA-256; `requirepass`, `AUTH default`, and
  `ACL SETUSER default >pass` converge on the same effective credential
  [acl-default-user].
- A single-key GET/SET on an authenticated connection takes one branch for the
  auth check and no shared lookup (the hot-path lint, ADR-0002).

## References

- ADR-0002, ADR-0007, ADR-0009; issues #22, #15, #18, #105, #106, #142, #97, #1;
  specs PROTOCOL.md, ERRORS.md, TESTING.md.
- Claims: [acl-default-user], [hello-noproto-error], [client-default-resp3-redis8],
  [valkey-license-bsd3], [valkey-resp-identical].
