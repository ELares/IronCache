# Security Policy

IronCache is in its research and specification phase and has no shipping code
yet. There is therefore no released artifact to report a vulnerability against.
This policy is published early so the reporting path exists from day one.

## Reporting a vulnerability

Please report suspected security issues privately. Do not open a public issue
for a vulnerability.

- Preferred: use GitHub's private vulnerability reporting on this repository
  (the "Report a vulnerability" button under the Security tab).
- Alternatively, email the maintainers at security@ironcache.dev.

You will receive an acknowledgement, and we will work with you on a coordinated
disclosure timeline. We will credit reporters who wish to be credited.

## Scope

The threat model (an authenticated, network-facing cache that speaks the Redis
wire protocol, with optional TLS, persistence, and clustering) is documented in
`docs/THREAT_MODEL.md`, with the per-subsystem controls and their honest
implementation status in `docs/design/` (AUTH.md, ACL.md, TLS.md, SECRETS.md,
HARDENING.md). As IronCache moves toward a release this policy will be expanded to
cover supported versions.

## Secret material in memory

Defense-in-depth posture for plaintext secret lifetime in process memory (#145;
the authoritative detail is `docs/design/SECRETS.md` "Implementation status" and
the THREAT_MODEL "Process memory (host-local)" row).

What IS protected:

- AUTH `requirepass` and ACL passwords are stored as SHA-256 hex digests AT REST,
  not plaintext, and compared in constant time. There is no long-lived plaintext
  password held anywhere.
- Secret command arguments are redacted from SLOWLOG, MONITOR, INFO, and logs
  (`AUTH`/`HELLO AUTH`/`CONFIG SET requirepass`/`ACL SETUSER >pass`).
- The long-lived `cluster_secret` (the one secret that cannot be reduced to a hash,
  because the peer handshake compares its literal bytes) is wrapped in `Zeroizing`
  so it is scrubbed from the heap when the node tears down.
- The transient plaintext copy a `CONFIG SET requirepass` materializes is wrapped
  in `Zeroizing` and scrubbed immediately after it is hashed.
- TLS private-key material is zeroized by rustls itself; IronCache does not copy it
  out, so there is nothing to double-handle.

What is NOT protected, and why:

- The transient plaintext of `AUTH`/`HELLO AUTH`/`ACL SETUSER >pass` is not
  explicitly scrubbed. It lives only as an immutable, refcount-shared decoded
  argument and in the reused connection read buffer; the auth path hashes it by
  reference without copying it out. Clearing the shared read buffer would risk
  pipelining correctness and add a wipe to a near-hot path for a marginal gain, so
  it is an accepted residual. The threat model assumes an attacker with live
  process-memory access (a core dump, swap, `/proc/<pid>/mem`) is already past the
  at-rest protections; Rust memory safety and no-secrets-in-diagnostics are the
  primary controls.
- OS-level core-dump and swap hardening (`mlock`, `MADV_DONTDUMP`, `RLIMIT_CORE`)
  is not yet wired in-process. The paranoid operator should disable core dumps and
  swap at the OS level (`ulimit -c 0`, no swap or encrypted swap).
