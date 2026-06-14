# Design: Embedded rustls TLS listener

Issue: #105 (spun out of #22). Decisions: ADR-0017 (Simple gate: musl static,
kernel-only, no sidecar), ADR-0009 (behavioral equivalence). Related: #22 (parent),
#104/AUTH.md (auth runs inside the TLS-wrapped connection), #81/CLI_BINARY.md
(single static binary), #85/CONFIG.md (cert/key/port config), #8/BENCHMARK.md
(the overhead experiment), #142 (threat model).

## Goal and scope

IronCache terminates TLS in-process with no external proxy and no C TLS library,
so the single static binary stays the whole deployment. This specifies the
embedded rustls listener: cert/key configuration, the TLS-only posture, optional
mutual-TLS, the protocol-version and cipher floor, and how the plaintext-vs-TLS
cost is measured (rather than inherited from vendor headlines). Out of scope: the
AUTH credential model (#104, runs inside the TLS session) and the full ACL engine
(#106).

## Design

### Embedded rustls, no C TLS library

- TLS is terminated by rustls, a pure-Rust TLS library implementing TLS 1.2 and
  1.3 with no dependency on OpenSSL or any other C TLS library
  [rustls-pure-rust-tls12-tls13]. This preserves the Simple-gate constraint
  (ADR-0017, CLI_BINARY.md #81): the musl static binary links no system OpenSSL,
  so TLS adds no C dependency, no sidecar, and no extra process. rustls is
  safe-by-default: it excludes SSLv3/TLS1.0/TLS1.1 by construction
  [rustls-pure-rust-tls12-tls13], so the version floor is TLS 1.2 and the cipher
  suites are rustls's vetted defaults rather than an operator-tuned list. TLS wraps
  the transport only: the RESP observable contract is unchanged (ADR-0009
  behavioral equivalence), so a client sees identical replies over TLS or plaintext.

### Cryptographic backend and the static-binary trade

- rustls selects its crypto provider by crate feature: aws-lc-rs (default,
  AWS-LC/BoringSSL-based, available as a FIPS-certified provider, but pulls a cmake
  build dependency) or ring (easier cross-builds); both are Rust crates, so neither
  links system OpenSSL [rustls-aws-lc-rs-default-ring-alternative]. IronCache pins
  one provider for the reproducible static build; the choice (aws-lc-rs for a
  FIPS-certified backend vs ring for build simplicity under cargo-zigbuild) is a
  build-config decision tied
  to #81/#84, defaulting to the provider that keeps the musl cross-build
  reproducible, with `default-features = false` used to keep the dependency
  explicit [rustls-aws-lc-rs-default-ring-alternative].

### Cert/key config and the TLS-only posture

- The listener takes a cert chain and private key from configured paths (#85);
  cert/key reload (so a cert rotation needs no restart) is a hot-swappable
  parameter this spec adds to the CONFIG.md #85 partition, distinct from the
  restart-required bind/port. TLS is exposed
  on a dedicated `tls-port` (a separate listener) rather than by sniffing TLS vs
  plaintext on one port: this resolves the #22 open decision toward separate-port,
  which is unambiguous and avoids a per-connection protocol-detection heuristic. A
  TLS-only posture is then `tls-port` enabled with the plaintext port disabled, so
  plaintext is refused by not being listened on rather than accepted and rejected.

### Optional mutual TLS

- Client-certificate verification (mTLS) is an optional mode: when a CA bundle is
  configured the listener requires and verifies a client cert, otherwise it is
  server-auth only. mTLS authenticates the transport peer; it is orthogonal to the
  AUTH/ACL identity (#104/#106), which still applies inside the session.

### Measured overhead, not inherited headlines

- TLS throughput numbers are workload-specific, so IronCache measures its own
  plaintext-vs-TLS QPS-per-core delta on the harness (BENCHMARK.md #8) rather than
  quoting a vendor figure. KeyDB markets ~7x throughput with TLS versus Redis
  [keydb-tls-7x-claim] and Valkey's async-I/O work reports large TLS-workload gains
  [valkey-async-io-throughput], but those are TLS-offload/threading headlines tied
  to those engines, not a number IronCache can inherit. The experiment is defined
  now and run when the engine and harness exist (the same harness-deferred
  discipline as the other M1 benchmarks): same hardware, same key/value mix,
  plaintext vs TLS 1.3, reporting the per-core throughput and tail-latency delta.

## Open questions

- The pinned crypto provider (aws-lc-rs vs ring) for the reproducible static
  build, decided with the cross-build/packaging work (#84) and the FIPS
  requirement, if any.
- Whether mTLS ships in the first TLS build or follows with the full ACL (#106).
- Session resumption (tickets vs IDs) and whether 0-RTT is ever enabled (replay
  risk), deferred with the threat model (#142).

## Acceptance and test hooks

- The listener accepts a TLS 1.3 (and 1.2) connection with a configured cert/key
  and serves RESP over it; SSLv3/TLS1.0/TLS1.1 are refused
  [rustls-pure-rust-tls12-tls13]; the dependency tree links no C TLS library
  [rustls-aws-lc-rs-default-ring-alternative] (a `cargo tree` / SBOM check, #84).
- TLS-only mode (plaintext port disabled) refuses a plaintext connection by not
  listening; a cert rotation is picked up by reload with no restart (#85).
- With mTLS configured, a client without a valid cert is rejected at the TLS layer
  before AUTH; with it off, server-auth-only connections succeed.
- The harness reports a plaintext-vs-TLS per-core throughput and tail-latency
  delta on identical hardware (BENCHMARK.md #8), contextualizing rather than
  inheriting the vendor TLS headlines [keydb-tls-7x-claim][valkey-async-io-throughput].

## References

- ADR-0009, ADR-0017; issues #22, #104, #106, #81, #84, #85, #8, #142, #1;
  specs AUTH.md, CLI_BINARY.md, CONFIG.md, BENCHMARK.md.
- Claims: [rustls-pure-rust-tls12-tls13], [rustls-aws-lc-rs-default-ring-alternative],
  [keydb-tls-7x-claim], [valkey-async-io-throughput].
