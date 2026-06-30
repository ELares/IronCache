# ADR-0020: CLI mode dispatch and artifact signing

Status: Accepted
Issue: #82, #119

## Context

IronCache ships as one static binary that is server, client, bench harness,
checker, config tool, and self-updater. How that binary selects its mode is a UX
and security decision upstream of packaging, install, and upgrade, and how its
artifacts are signed is a supply-chain decision. This settles both. (#119 is the
split child for the dispatch half; this ADR resolves both.)

## Decision

- **Dispatch via explicit clap subcommands**: `ironcache server | cli | bench |
  check | upgrade | config`, with global root flags (`--config`, `--bind`,
  `--port`, `--log-level`, `--metrics-addr`). For ergonomics, if the binary is
  invoked as `redis-cli` (a named alias/symlink) it forwards to `ironcache cli`,
  but that is a convenience, not the primary dispatch.
- **Sign release artifacts with minisign**: a detached minisign signature per
  artifact plus an embedded reproducible-build SBOM (cargo-auditable
  [cargo-auditable-version-reproducible]). The public key ships in the repo and
  docs so `upgrade` and any operator can verify on any box with no PKI.

## Rejected Alternatives

- **argv[0] symlink mode-switching (Redis/KeyDB style).** Redis ships separate
  binaries with some as symlinks [redis-separate-binaries-symlinks] and KeyDB
  uses redis-compat symlinks [keydb-redis-compat-symlinks]. Rejected as the
  primary mechanism: subcommands are self-documenting (`ironcache --help`),
  discoverable, and avoid a pile of install-time symlinks; Dragonfly already
  ships a single self-contained binary with flag dispatch
  [dragonfly-single-binary-gflags]. The `redis-cli` alias is kept only for muscle
  memory.
- **cosign/sigstore signing.** Powerful (transparency log, keyless OIDC) but it
  pulls a heavier verify-time dependency and assumes network/Fulcio access;
  minisign verifies offline with a single small public key, which fits the
  Simple tenet and the offline-installer/upgrade story.

## Consequences

- The single-binary design (#81), upgrade-with-rollback (#83), and packaging
  (#84) build on clap subcommands and minisign verification.
- `upgrade` verifies the minisign signature (and the SBOM) before swapping the
  binary; an unverified artifact is refused (the rollback story is #83).
- The CLI surface is testable (`--help` snapshot, each subcommand) and the
  `redis-cli` alias forwards correctly.

## Amendment (#386, finalized 2026-06-29): verification anchor confirmed = minisign

Issue #386 reopened the anchor question because the release pipeline as it actually
runs publishes, on the rolling channel (every push to main), a consolidated
`SHA256SUMS` plus a KEYLESS Sigstore (cosign) build-provenance attestation, and NO
minisign signature, with no minisign public key committed. The alternative on the
table (option A) was to verify against what currently ships: SHA256 against
`SHA256SUMS` plus `cosign verify-blob-attestation` pinning the build-workflow
identity + OIDC issuer.

Decision (finalized): keep **minisign** (option B), as this ADR's Decision and
Rejected Alternatives already specify. Rationale unchanged: a detached minisign
signature is verified OFFLINE with a single small committed Ed25519 public key (no
PKI, no Fulcio/transparency-log network dependency at verify time), which fits the
Simple tenet and the musl + cargo-deny posture (ADR-0017, the same rationale the
hand-rolled SHA-256 documents). Cosign's keyless OIDC is powerful but pulls a
heavier verify-time dependency and a network assumption the offline upgrade story
rejects.

Sequencing (mechanism first, sign next): the self-updater spine (#387) ships now
with `Sha256Verifier` (INTEGRITY: the bytes match the published `SHA256SUMS`)
behind the `upgrade::verify::Verifier` trait. The cryptographic AUTHENTICITY anchor
is a `MinisignVerifier` implementing that SAME trait (no orchestrator change), the
named follow-up tracked on #386.

Operational requirements this confirms (the gap #386 surfaced):
- the release workflow MUST produce a per-binary detached minisign signature
  (over the artifact, or over `SHA256SUMS`) alongside the existing `SHA256SUMS`;
- a pinned minisign Ed25519 PUBLIC key MUST be committed to the repo + docs so
  `upgrade` (and any operator) can verify on any box with no PKI;
- the rolling channel's cosign attestation is retained as supplementary build
  provenance, but it is NOT the `upgrade` verify anchor.
