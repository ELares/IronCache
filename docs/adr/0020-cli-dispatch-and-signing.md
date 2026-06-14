# ADR-0020: CLI mode dispatch and artifact signing

Status: Accepted
Issue: #82

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
