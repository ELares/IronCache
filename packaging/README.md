<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Packaging scaffolds (templated, INERT until the first release)

These files implement the install and distribution surface specified in
[`docs/design/PACKAGING.md`](../docs/design/PACKAGING.md) (#84 and its children
#121 / #122 / #123 / #125).

**Status: scaffold.** The repo is docs-only today: there is no `Cargo.toml`, no
`Cargo.lock`, and no built binary yet. Every file here is a template carrying
placeholders that the release pipeline fills at the first tagged release:

- `__VERSION__` is the release version (for example `0.1.0`).
- `__SHA256_*__` are the per-artifact SHA256 digests, written from the release
  `SHA256SUMS`.
- `OWNER/REPO` is the GitHub repo slug for the release download base.

Nothing here runs or is published until `.github/workflows/release.yml` is
triggered by a `v*` tag, and that workflow short-circuits while no `Cargo.toml`
exists. This mirrors the design-now / build-at-implementation posture that
`docs/design/SUPPLY_CHAIN.md` uses for `deny.toml`.

## Contents

| File | Purpose | Issue |
| --- | --- | --- |
| `install.sh` | Owned, pinned `curl \| sh` installer; SHA256-validates before unpacking, manages PATH. | #122 |
| `Formula/ironcache.rb` | Plain Homebrew formula (no tap, no cask, no conflict). | #122 |
| `Dockerfile` | Distroless-static / scratch image under 5 MiB, runs nonroot. | #122 |
| `ironcache.service` | Hardened systemd unit (eight defense-in-depth directives). | #122 |
| `../.github/workflows/release.yml` | cargo-zigbuild cross-build matrix, CycloneDX SBOM, SHA256SUMS, minisign. | #121 #123 |

Artifact signing (minisign) and the *embedded* reproducible SBOM
(cargo-auditable) are owned by ADR-0020 and `docs/design/SUPPLY_CHAIN.md`; the
per-release CycloneDX SBOM export and the SHA256 attestation layer are added
here.
