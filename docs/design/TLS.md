# Design: Embedded rustls TLS listener

Issue: #105 (spun out of #22). Decisions: ADR-0017 (Simple gate: musl static,
kernel-only, no sidecar), ADR-0009 (behavioral equivalence). Related: #22 (parent),
#104/AUTH.md (auth runs inside the TLS-wrapped connection), #81/CLI_BINARY.md
(single static binary), #85/CONFIG.md (cert/key/port config), #8/BENCHMARK.md
(the overhead experiment), #142 (threat model).

## Shipped status (reconciliation with the code)

This is the original DESIGN spec (#105). The implementation shipped, and a few of the
forward-looking choices below resolved DIFFERENTLY from what this document sketched.
The operator guide is `docs/TLS.md`; this section reconciles the design with the code
so a reader does not take a sketch for current truth:

- **Crypto provider: `ring` (not aws-lc-rs).** The "aws-lc-rs vs ring" open question is
  RESOLVED in favor of `ring`, pinned at the workspace level for a reproducible
  musl / aarch64 static cross-build (no cmake, no C toolchain). See
  `crates/ironcache-runtime/src/tls.rs`.
- **Certificate rotation needs a RESTART (no hot reload).** The "cert/key reload with
  no restart" this spec proposed did NOT ship: the acceptor is built once at boot and
  cloned onto every shard (`crates/ironcache/src/serve.rs`). Hot reload is tracked as a
  follow-up (issue #563). Rotate with a rolling restart (`docs/TLS.md`).
- **Client-port mTLS did NOT ship.** The public client listener is server-auth ONLY
  (`build_acceptor` uses `with_no_client_auth()`); client identity is established by
  AUTH / ACL inside the session. Full client-cert mTLS on the client port is a
  follow-up.
- **A single TLS-only client port, not a separate `tls-port`.** The shipped model is
  `tls = off | on` on the one client port (TLS-only when on); a second plaintext port
  alongside a `tls-port` is a documented follow-up (`crates/ironcache-config/src/lib.rs`).
- **The intra-cluster (bus + replication) links DID ship TLS (PROD-3),** which this
  spec listed only as a follow-up. `cluster_tls = on` encrypts the RAFTMSG bus and the
  replication stream, with a shared-secret peer handshake and optional CA-verified peer
  certs (`crates/ironcache-clusterbus/src/security.rs`,
  `crates/ironcache/src/raft_boot.rs`). By default those links are plaintext +
  unauthenticated and a loud boot warning fires (`crates/ironcache/src/cluster_bus.rs`).

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
  explicit [rustls-aws-lc-rs-default-ring-alternative]. RESOLVED: `ring` is the
  pinned provider (the reproducible cross-build won over the FIPS backend), so
  IronCache does NOT depend on aws-lc-rs and needs no cmake / C toolchain to build
  the TLS binary (`crates/ironcache-runtime/src/tls.rs`).

### Cert/key config and the TLS-only posture

- The listener takes a cert chain and private key from configured paths (#85).
  This spec proposed cert/key reload (a cert rotation with no restart) as a
  hot-swappable parameter; that did NOT ship (see "Shipped status" above): the
  acceptor is built once at boot, so rotation needs a restart today (issue #563).
  TLS is exposed
  by making the ONE client port TLS-only when `tls = on` (plaintext clients fail the
  handshake and are dropped), rather than by sniffing TLS vs plaintext on one port.
  (This spec originally proposed a dedicated separate `tls-port` alongside the
  plaintext port; the shipped model is the single `tls = off | on` client port, and a
  both-ports posture is a documented follow-up, `crates/ironcache-config/src/lib.rs`.)

### Optional mutual TLS

- Client-certificate verification (mTLS) on the PUBLIC client port did NOT ship: the
  listener is server-auth ONLY (`build_acceptor` uses `with_no_client_auth()`), and
  client identity is established by AUTH / ACL inside the session (#104/#106). Full
  client-cert mTLS on the client port is a follow-up. The INTRA-CLUSTER links DO
  verify the peer's server cert against a cluster CA (`cluster_ca_path`) plus a
  shared-secret peer handshake, which is the cluster's mutual-authentication story
  (see "Shipped status" and `docs/TLS.md`).

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

- ~~The pinned crypto provider (aws-lc-rs vs ring).~~ RESOLVED: `ring` (see "Shipped
  status").
- ~~Whether mTLS ships in the first TLS build.~~ RESOLVED: client-port mTLS did NOT
  ship (server-auth only); the cluster links use CA-verified peer certs + a shared
  secret. Client-port client-cert mTLS remains a follow-up.
- Restart-free cert rotation (hot reload): a follow-up (issue #563); today rotation
  needs a restart.
- Session resumption (tickets vs IDs) and whether 0-RTT is ever enabled (replay
  risk), deferred with the threat model (#142).

## Acceptance and test hooks

- The listener accepts a TLS 1.3 (and 1.2) connection with a configured cert/key
  and serves RESP over it; SSLv3/TLS1.0/TLS1.1 are refused
  [rustls-pure-rust-tls12-tls13]; the dependency tree links no C TLS library
  [rustls-aws-lc-rs-default-ring-alternative] (a `cargo tree` / SBOM check, #84).
- TLS-only mode (`tls = on`) refuses a plaintext connection at the handshake. (A cert
  rotation currently needs a restart, NOT a live reload; hot reload is issue #563.)
- Server-auth-only connections succeed; the public client port does NOT verify a
  client cert (mTLS on the client port is a follow-up). The intra-cluster dial rejects
  a peer whose cert is not signed by the configured cluster CA.
- The harness reports a plaintext-vs-TLS per-core throughput and tail-latency
  delta on identical hardware (BENCHMARK.md #8), contextualizing rather than
  inheriting the vendor TLS headlines [keydb-tls-7x-claim][valkey-async-io-throughput].

## References

- Operator guide (how to run it): `docs/TLS.md`.
- ADR-0009, ADR-0017; issues #22, #104, #106, #81, #84, #85, #8, #142, #563, #1;
  specs AUTH.md, CLI_BINARY.md, CONFIG.md, BENCHMARK.md.
- Claims: [rustls-pure-rust-tls12-tls13], [rustls-aws-lc-rs-default-ring-alternative],
  [keydb-tls-7x-claim], [valkey-async-io-throughput].
