# Design: Full ACL engine and aclfile persistence

Issue: #106 (spun out of #22). Decisions: ADR-0009 (compat tiering, ACL is a
compatibility surface), ADR-0002 (shared-nothing, per-connection identity off the
data path). Related: #104/AUTH.md (the M1 default-user this extends), #22 (parent
security umbrella), #142 (threat model), #85/CONFIG.md (aclfile path and reload).

## Goal and scope

This specifies the full Redis-compatible ACL engine as an additive superset of the
M1 default-user (AUTH.md): named users, command and key and channel permissions,
selectors, and `aclfile` persistence. The build is deferred past M1 (the M1 slice
ships only the degenerate single default-user); the shape is fixed now so the later
implementation is additive, not a rewrite of the auth path. Scope: the ACL command
surface, the permission model, and aclfile load/save. The handshake and credential
storage are AUTH.md (#104); the transport is TLS (#105).

## Design

### Permission model (superset of the default user)

- A user is `on`/`off`, a set of passwords (SHA-256 hashes, `>pass`/`#hash`), a
  command rule set, a key-pattern set, and a channel-pattern set
  [acl-default-user]. The M1 default user `on nopass ~* &* +@all` (AUTH.md) is the
  all-permissive degenerate case of exactly this model, so adding the engine adds
  users and narrows rules without changing the default user's behavior.
- Command rules: `+cmd`/`-cmd` for individual commands and `+@category`/
  `-@category` for the command categories (read, write, admin, dangerous, and the
  rest of the ~20 Redis categories) [acl-default-user]. Key rules: `~pattern`,
  with the read/write-scoped `%R~`/`%W~` forms (Redis 7.0+) [acl-default-user].
  Channel rules: `&pattern` for pub/sub channel access [acl-default-user].
- Selectors (Redis 7.0+): a user may carry multiple permission selectors
  `(+cmd ~key)` so a single identity can hold distinct command/key bundles, the
  superset form the default user degenerates out of [acl-default-user].

### Command surface

- `ACL SETUSER`, `GETUSER`, `DELUSER`, `LIST`, `CAT`, `GENPASS`, `WHOAMI`, and
  `USERS` [acl-default-user]. `GETUSER` reports the user in the Redis-recognized
  field shape so existing tooling parses it (verified differentially, not asserted
  here); `CAT` lists the command categories; `GENPASS` returns a CSPRNG hex secret.
- Enforcement is a per-connection check (ADR-0002): the connection resolves to a
  user at auth time (AUTH.md), and each command is gated against that user's
  resolved command/key/channel rules on the owning core, with no shared lookup on
  the hot path.

### aclfile persistence

- Users persist to an `aclfile` (one `user ...` line per user) that is loaded at
  startup and rewritten by `ACL SAVE`, mirroring Redis's `aclfile` model; the file
  path is a config knob (CONFIG.md #85) and reloadable. `ACL LOAD` re-reads it.
  When no aclfile is configured, only the default user exists (the M1 posture), so
  aclfile is purely additive.

### Compatibility tier and deferral

- ACL is a compatibility surface (ADR-0009): behavioral equivalence on the command
  shapes and error tokens, verified against the pinned Valkey oracle
  [valkey-resp-identical] [valkey-license-bsd3] (TESTING.md). The engine build is
  deferred past M1; this spec is the contract the deferred task implements so it
  extends, rather than rewrites, the AUTH default-user path.

## Open questions

- Whether `%R`/`%W` key-permission enforcement ships with the first ACL build or
  follows the coarse `~pattern` form (Redis added `%R%W` in 7.0; both are in the
  pinned shape).
- The aclfile-vs-CONFIG precedence when both define users (Redis forbids mixing;
  match that), settled with the CONFIG layering (#85).
- ACL attempt/error rate limiting, shared with the AUTH open question (#104) and
  informed by the threat model (#142).

## Acceptance and test hooks

- `ACL SETUSER`/`GETUSER`/`DELUSER`/`LIST`/`CAT`/`GENPASS`/`WHOAMI`/`USERS` match
  the pinned oracle reply shapes (#97); `GETUSER` field order/contents parse with
  unmodified tooling [acl-default-user].
- A user restricted by `+@read ~cache:*` can run `GET cache:x` but is denied
  `SET other:y` and a disallowed channel, with the Redis-exact `-NOPERM` token
  (ERRORS.md).
- An `aclfile` round-trips: users defined, `ACL SAVE`, restart, `ACL LOAD`, and the
  user set is identical (a persistence test).
- The default-user-only configuration (no aclfile) behaves exactly as the M1
  AUTH.md default user (a no-regression test), proving the engine is additive.

## References

- ADR-0002, ADR-0009; issues #104, #22, #142, #85, #97, #1; specs AUTH.md,
  ERRORS.md, CONFIG.md, TESTING.md.
- Claims: [acl-default-user], [valkey-resp-identical], [valkey-license-bsd3].
