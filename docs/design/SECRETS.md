# Design: Secrets handling (diagnostic redaction, in-memory zeroization, no swap, no coredump)

Issue: #145 (child of the #142 threat model). Decisions: ADR-0009 (behavioral
equivalence: the SHA-256 storage form and the diagnostic command shapes are kept,
so we redact rather than change them), ADR-0017 (Simple gate: musl static,
kernel-only, no sidecar, so the swap/coredump hardening is in-process). Related:
#142/THREAT_MODEL.md (the assets and accepted risks this discharges), #104/AUTH.md
(credential material), #105/TLS.md (key material), #86/OBSERVABILITY.md (the
diagnostic surfaces and the `/metrics` auth open question), #85/CONFIG.md
(`CONFIG SET` secret args and the hardening knobs), #106/ACL.md (user secrets).

## Goal and scope

IronCache holds AUTH/requirepass hashes [acl-default-user], configured passwords,
and TLS private keys [rustls-pure-rust-tls12-tls13] in process memory, and it
exposes diagnostic surfaces (SLOWLOG, MONITOR, INFO, logs, `/metrics`) that echo
command arguments. This spec discharges the threat-model rows for secret leakage
via diagnostics and via host-local memory (#142): redact secret arguments from
every diagnostic, zeroize key and password material on drop, keep the plaintext
keyspace and keys out of swap and coredumps, and decide whether `/metrics` and
MONITOR require auth. It is the concrete child of #142 and is distinct from the
auth handshake (#104) and TLS transport (#105). Out of scope: at-rest encryption
of snapshot/tier files (a separate threat-model line item, #142).

## Design

### Redaction of secret arguments from diagnostics

- The argument values of secret-bearing commands are redacted before they reach
  SLOWLOG, MONITOR, INFO, or any log line: `AUTH <pass>`, `AUTH <user> <pass>`,
  `HELLO <ver> AUTH <user> <pass>`, `CONFIG SET requirepass <pass>`,
  `CONFIG SET masterauth/masteruser`, `ACL SETUSER ... >pass/#hash`, and the
  replica auth token. The command name is preserved; only the secret positions
  are replaced with a fixed placeholder, so SLOWLOG keeps its threshold-10000us/
  128-entry shape [redis-slowlog-defaults] and MONITOR keeps its reply shape but
  carries no plaintext secret. This is a redaction layer over the existing
  diagnostic pipeline (OBSERVABILITY.md, #86), not a change to the command
  contract (ADR-0009): the wire reply of the command itself is unchanged, only
  the recorded/streamed copy is scrubbed.
- The redaction set is a per-command argument map, not a string search, so a
  password that happens to look like a keyword is still redacted by position. New
  secret-bearing commands register their secret argument indices in one place.

### In-memory zeroization on drop

- TLS private keys [rustls-pure-rust-tls12-tls13] and password material (the
  configured `requirepass`/ACL plaintext seen during a `CONFIG SET`/`SETUSER`
  before it is hashed to SHA-256 [acl-default-user]) are wrapped in a type that
  zeroizes its backing bytes when dropped, via the `zeroize` crate, which
  overwrites with a volatile write the compiler may not elide and integrates
  with `Drop` for automatic clearing [zeroize-crate-on-drop]. The stored hash is
  not secret-equivalent but is treated as sensitive and zeroized on the same path.
- Zeroization covers the transient plaintext that exists between parse and hash,
  and the long-lived key bytes when a cert is rotated (TLS.md cert/key reload,
  #105) or a config layer is cleared (CONFIG.md precedence, #85). It does not and
  cannot cover copies the allocator or kernel made; that residual is bounded by
  the swap/coredump measures below and recorded as an accepted residual in #142.

### Keeping secrets out of swap

- The process can lock its sensitive pages into RAM so the kernel never writes
  them to swap, using `mlock`/`mlockall` to pin pages and prevent paging to disk
  [mlock-pages-out-of-swap]. IronCache exposes this as a hardening config knob
  (#85): an opt-in `mlockall` for the whole address space (simplest, costs RSS
  headroom and needs `RLIMIT_MEMLOCK`), or page-scoped locking of the secret
  allocations. Because io_uring/monoio already needs memlock configured
  appropriately for the runtime, the RLIMIT_MEMLOCK posture is a shared operator
  prerequisite (RUNTIME.md, ADR-0017) rather than a new ask. When the limit is
  too low the server logs the downgrade rather than failing silently.

### Keeping secrets out of core dumps

- A coredump of the process would write the plaintext keyspace and keys to disk,
  so IronCache suppresses dumps of sensitive memory: per-region with
  `madvise(MADV_DONTDUMP)` on the secret/keyspace mappings, and process-wide as a
  belt-and-braces default with `prctl(PR_SET_DUMPABLE, 0)` and/or `RLIMIT_CORE`
  set to 0 to disable core dumps entirely
  [madv-dontdump-and-rlimit-core-no-coredump]. The default posture is dumps
  disabled (Simple gate, ADR-0017); an operator who needs a dump for debugging
  flips a hardening knob (#85), accepting the exposure for that run. These are
  Linux mechanisms; on a non-Linux fallback the knob reports unsupported rather
  than pretending to protect.

### Whether /metrics and MONITOR require auth (#86)

- MONITOR streams every command and so is treated as a privileged diagnostic: it
  requires an authenticated connection and, under the full ACL (#106), an
  explicit administrative permission; redaction (above) still applies to its
  stream as defense in depth. This resolves the threat-model elevation/info-
  disclosure row for MONITOR (#142).
- `/metrics` exposes counters/gauges (no command arguments, no secret values by
  the registry's construction, OBSERVABILITY.md #152), so the leak risk is
  internal-state cardinality, not secrets. Whether it is unauthenticated on a
  separate `--metrics-addr` bound to a trusted interface, or gated behind auth,
  is an open transport sub-decision in OBSERVABILITY.md (#86 is closed; that
  sub-question is not yet settled); this spec records the requirement that
  whichever transport is chosen, no secret-valued series is ever registered.

## Open questions

- Page-scoped locking of just the secret allocations versus `mlockall` of the
  whole space (RSS cost versus completeness), measured against the keyspace size.
- Whether `/metrics` is unauthenticated on a trusted-bound address or auth-gated,
  decided with the open transport sub-question in OBSERVABILITY.md (#86).
- Whether the redaction placeholder is byte-identical to Redis's `(redacted)`
  form for tooling that parses MONITOR/SLOWLOG, checked against the pinned oracle.
- AUTH attempt-rate limiting interaction (logging of repeated `-WRONGPASS`
  without leaking the attempted secret), shared with AUTH.md/#142.

## Acceptance and test hooks

- A test replays `AUTH`, `HELLO AUTH`, `CONFIG SET requirepass`, and
  `ACL SETUSER >pass` and asserts no plaintext secret appears in SLOWLOG,
  MONITOR, INFO, or the log sink, while the command name and reply shape are
  unchanged (ADR-0009, [redis-slowlog-defaults]).
- A drop/leak test asserts key and password buffers read as zeroed after drop
  (`zeroize` [zeroize-crate-on-drop]); a hot-path lint asserts secret types are
  never logged via their `Debug`/`Display`.
- With the hardening knobs on, a forced coredump produces no dump (or a
  dump with the sensitive regions absent) [madv-dontdump-and-rlimit-core-no-coredump],
  and the secret pages are reported locked/unswappable [mlock-pages-out-of-swap];
  on a non-Linux target the knob reports unsupported.
- MONITOR refuses an unauthenticated connection; the metric registry contains no
  secret-valued series (a registry assertion, OBSERVABILITY.md #152).

## References

- ADR-0009, ADR-0017; issues #145, #142, #104, #105, #86, #85, #106, #152, #1;
  specs THREAT_MODEL.md, AUTH.md, TLS.md, OBSERVABILITY.md, CONFIG.md, ACL.md.
- Claims: [zeroize-crate-on-drop], [mlock-pages-out-of-swap],
  [madv-dontdump-and-rlimit-core-no-coredump], [acl-default-user],
  [rustls-pure-rust-tls12-tls13], [redis-slowlog-defaults].
