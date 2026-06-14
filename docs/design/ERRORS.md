# Design: Redis-compatible error-string catalog

Issue: #18. Decisions: ADR-0009 (behavioral equivalence), ADR-0019 (error
fidelity). Related: #15 (protocol), #95 (conformance).

## Goal and scope

Error strings are part of the Redis wire contract, not cosmetic text. Mainstream
clients pattern-match on the leading uppercase token (and sometimes the full
message) to drive control flow: handshake fallback, auth retries, transaction
aborts, type-error surfacing. The wrong token or wording breaks unmodified
clients in ways no reply-type test catches. This defines one canonical error
catalog as a first-class part of the protocol layer.

In scope: the canonical prefixes and their exact text, the mapping from internal
error conditions onto the catalog, and the handshake-critical errors (unknown
command, arity, wrong proto, auth). Out of scope: cluster redirection errors
(`MOVED`/`ASK`, owned by #70) and command-specific messages beyond the shared
prefixes (owned per command in #128/#129).

## Design

### Canonical prefixes

A single `ErrorCode` enum maps to exactly these leading tokens, each with Redis-
identical text for the shared cases:

- `ERR` generic (unknown command, wrong number of arguments, syntax).
- `WRONGTYPE` operation against a key holding the wrong kind of value.
- `NOPROTO` unsupported `HELLO` protocol version [hello-noproto-error].
- `NOAUTH` authentication required; `WRONGPASS` invalid username/password.
- `NOPERM` ACL permission denied [acl-default-user].
- `EXECABORT` transaction discarded due to previous errors (with the no-rollback
  semantics of ADR-0010 [multi-exec-no-rollback]).
- `BUSYKEY`, `OUTOFRANGE`, `NOTBUSY` and the other shared tokens as their
  conditions are implemented.

### Fidelity rule

The leading token is byte-identical to Valkey in all cases; the full message
text is byte-identical for the handshake-critical and control-flow errors
(`NOPROTO`, `NOAUTH`/`WRONGPASS`, `EXECABORT`, `WRONGTYPE`, unknown-command,
arity). For purely informational messages the token matches and the text is
behaviorally equivalent (ADR-0009). The differential oracle (#97) enforces the
chosen bar per message; where Valkey wording drifts across versions, the pinned
oracle version wins.

### Pinned verbatim strings (handshake-critical and control-flow)

These exact strings are pinned in the catalog (not deferred to the oracle),
because clients pattern-match them:

- `WRONGTYPE Operation against a key holding the wrong kind of value`
- `ERR unknown command '<name>', with args beginning with: ...`
- `ERR wrong number of arguments for '<command>' command`
- `EXECABORT Transaction discarded because of previous errors.`
- `NOPROTO` uses the EMITTED server string [hello-noproto-error], which is
  `NOPROTO unsupported protocol version` (from helloCommand in
  src/networking.c). Note the RESP3 spec prose ("sorry, this protocol version is
  not supported.") is documentation wording the server never sends; the catalog
  pins the emitted string.
- `NOAUTH Authentication required.` and `WRONGPASS invalid username-password
  pair or user is disabled.`

The pinned text tracks the oracle version (#96); a drift in upstream wording is a
deliberate catalog update, not silent.

### Internal mapping

Every internal error type (a typed Rust enum, never a stringly-typed error)
implements a total mapping to one `ErrorCode` plus its arguments. The serializer
(#15) renders `-<TOKEN> <message>\r\n`; no call site hand-writes an error string.
Unknown commands and arity violations emit the exact tokens clients expect during
startup so handshake fallback paths work.

## Open questions

- The exact-text-vs-leading-token boundary per non-handshake error (resolved
  case by case against the oracle as commands land).
- Whether to expose a `DEBUG`-style error-injection hook for conformance testing.

## Acceptance and test hooks

- A table test asserts each `ErrorCode` renders the pinned-Valkey token and text
  for the handshake-critical set.
- The differential suite (#97) compares error replies byte-for-byte (leading
  token always; full text for the control-flow set) against pinned Valkey.
- A client that pattern-matches `NOAUTH`/`WRONGPASS`/`EXECABORT`/`WRONGTYPE`
  drives its control flow unchanged.

## References

- ADR-0009, ADR-0019, ADR-0010; issues #15, #95, #97, #70.
- Claims: [hello-noproto-error], [acl-default-user], [multi-exec-no-rollback],
  [valkey-resp-identical].
