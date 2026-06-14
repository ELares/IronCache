# Threat model

Issue: #142. Decisions: ADR-0009 (behavioral equivalence sets some accepted
risks), ADR-0017 (Simple gate: musl static, kernel-only, no sidecar shrinks the
surface), ADR-0002 (shared-nothing, per-connection state). Related: #22 (parent
security epic), #104/AUTH.md, #105/TLS.md, #106/ACL.md, #145/SECRETS.md (the
secrets child), #137/ADMISSION.md, #138/HARDENING.md, #86/OBSERVABILITY.md,
#95/#100 (fuzz/fault-injection acceptance).

This is the shared adversary the rest of security hangs off, the file SECURITY.md
promises. It is an analysis doc, not a subsystem spec, so it adapts the house
template: assets, trust boundaries, attacker capabilities, STRIDE per subsystem,
then in-scope vs accepted-risk. STRIDE is the standard Microsoft framework and
needs no claim. Point mitigations live in their own specs, referenced not restated.

## Goal and scope

IronCache is an authenticated, network-facing cache speaking the Redis wire
protocol, with optional TLS, optional persistence/tiering, and a planned
replication/cluster layer. This document names what is worth protecting, who can
attack it and how, and which risks are mitigated versus consciously accepted. It
bounds the security-acceptance target for the fuzz and fault-injection stack
(#95/#100): a finding is in scope only if it maps to an asset and a modeled
attacker here.

## Design

### Assets

- In-RAM keyspace: plaintext keys and values in process memory (the hot tier).
  The primary asset; everything else protects it.
- Snapshot and tiered files: on-disk snapshot/AOF and the cold-tier SSD store
  (persistence and tiering land later in M1); plaintext at rest unless encrypted.
- AUTH credentials: the default-user password stored as SHA-256
  [acl-default-user] and any configured ACL user secrets, in memory and in
  `aclfile` (#106).
- TLS key material: the server private key and optional mTLS CA/client trust
  (#105), in memory and on disk.
- Replication stream: the bytes a primary ships to replicas and the replica
  auth token (#76, later in M1).
- Admin/metrics surface: `/metrics`, `MONITOR`, `SLOWLOG`, `INFO`, `CONFIG`, and
  the admin commands (#150) that expose state and arguments.

### Trust boundaries

- Untrusted RESP clients: anyone who can open the RESP port. Pre-auth they reach
  only the parser and the Tier 0 handshake (PROTOCOL.md, AUTH.md); post-auth
  their identity is the ACL user.
- Replica peers: a replica is semi-trusted; a compromised replica can read the
  full stream it is fed and may try to influence the primary (#76).
- Disk: the snapshot/tier/aclfile/cert files sit below the process; the host
  filesystem and anyone with read access to it are outside the process boundary.
- Host operator: root/operator on the host (swap, coredumps, `/proc`,
  ptrace) is trusted-but-modeled; we reduce blast radius (SECRETS.md) but do not
  defend against a malicious root.

### Attacker capabilities

- MITM on the wire: reads/modifies/replays traffic; defeated by TLS 1.2/1.3
  with SSLv3/TLS1.0/TLS1.1 refused [rustls-pure-rust-tls12-tls13] (#105).
  Plaintext mode trusts the network.
- Malicious authenticated or unauthenticated client: crafted/dribbled frames,
  oversized multibulk, deep nesting, connection floods; bounded by the parser
  limits (#138) and connection admission (#137).
- Compromised replica or replication MITM: reads the stream, replays, or feeds a
  hostile stream; bounded by replica auth + TLS on the replication link (#76).
- Host-local reader: reads swap, a coredump, or `/proc/<pid>/mem` to recover the
  plaintext keyspace or keys; reduced (not eliminated) by SECRETS.md mlock +
  no-coredump.

### STRIDE per subsystem

- Connection/parser (untrusted client edge): Tampering/DoS dominate. Malformed
  or amplification frames are bounded per-frame (#138) and per-connection (#137,
  maxclients/output-buffer/`-OOM`); a frame cannot exhaust a core's shard.
- AUTH/ACL: Spoofing/Elevation. `-NOAUTH`/`-WRONGPASS` gate every data command
  (AUTH.md), ACL scopes keys/commands/channels (#106). SHA-256 storage
  [acl-default-user] is weaker than a KDF: accepted for behavioral equivalence
  (below). AUTH attempt-rate limiting is an open AUTH.md/#142 question.
- TLS: Spoofing/Tampering/Info-disclosure. rustls floor + optional mTLS (#105);
  0-RTT/session-resumption replay risk is deferred to this model (#105 open
  question) and stays off until analyzed.
- Storage/persistence/tier: Info-disclosure/Tampering at rest. Plaintext files
  unless encrypted; at-rest encryption is tracked separately and listed as
  accepted-for-now below.
- Replication: Spoofing/Tampering/Info-disclosure. A peer that authenticates
  reads everything it is sent; auth + TLS on the link, fed-stream validation
  (#76).
- Observability/admin (`/metrics`, MONITOR, SLOWLOG, INFO, CONFIG, admin #150):
  Info-disclosure/Elevation. These can leak secret command arguments and
  internal state; redaction, and whether `/metrics`/MONITOR require auth, are
  owned by SECRETS.md (#145) and the open `/metrics`-transport question in
  OBSERVABILITY.md (#86, the spec is settled; this sub-decision is not).
- Process memory (host-local): Info-disclosure. Swap/coredump/`/proc` exposure
  of the keyspace and keys; mitigated by SECRETS.md zeroize/mlock/no-coredump,
  accepted against a malicious root.

### In-scope versus accepted risk

In scope (mitigated, with an acceptance target in #95/#100): network MITM
(TLS), malformed/amplification input (#138/#137), unauthenticated access
(AUTH/ACL), secret leakage via diagnostics and host-local memory (SECRETS.md).

Accepted (documented, not defended this milestone): a malicious host root;
SHA-256 password storage in place of a slow KDF, taken deliberately for Redis
behavioral equivalence (ADR-0009, AUTH.md); plaintext-at-rest until snapshot/tier
encryption ships; plaintext-wire when TLS is disabled by the operator; side
channels (timing/cache) on the cryptographic compare. Each accepted item is
revisited as its owning subsystem lands.

## Open questions

- AUTH attempt-rate limiting in M1 versus deferred with the full ACL (#106).
- Whether at-rest encryption for snapshot/tier/aclfile is an M1 asset or a later
  milestone, and the key-management model if so.
- Replication-link trust: mutual auth and stream validation depth against a
  compromised peer (#76), settled with the replication ADR.
- Whether 0-RTT is ever enabled given replay risk (#105).

## Acceptance and test hooks

- Every #95/#100 security finding maps to an asset and a modeled attacker here,
  or it is out of scope; this doc is the acceptance target.
- The parser/admission hardening (#138/#137) is fuzzed against the untrusted-
  client capabilities listed (#95).
- SECRETS.md (#145) discharges the diagnostic-leak and host-local-memory rows;
  AUTH.md/ACL.md/TLS.md discharge the spoofing/MITM rows; each accepted risk has
  a one-line rationale traceable to an ADR or owning spec.

## References

- ADR-0002, ADR-0009, ADR-0017; issues #22, #104, #105, #106, #145, #137, #138,
  #86, #150, #76, #95, #100, #1; specs AUTH.md, TLS.md, ACL.md, SECRETS.md,
  ADMISSION.md, HARDENING.md, OBSERVABILITY.md, PROTOCOL.md; SECURITY.md.
- Claims: [acl-default-user], [rustls-pure-rust-tls12-tls13].
