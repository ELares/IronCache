<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
# Packaging

These files implement the install and distribution surface specified in
[`docs/design/PACKAGING.md`](../docs/design/PACKAGING.md) (#84 and its children
#121 / #122 / #123 / #125).

**Status: live, with two dead scaffolds.** The workspace is real and releases
SHIP: `v0.1.0` is tagged, `.github/workflows/release.yml` publishes signed
tarballs + `SHA256SUMS` + a CycloneDX SBOM on every `v*` tag, and
`.github/workflows/rolling-release.yml` cuts a CalVer release on every push to
`main`. Not everything in this directory is wired to that pipeline yet; the
table below is honest about which is which.

| File | Status |
| --- | --- |
| `ironcache.service` | **Install-ready.** Hardened systemd unit; install steps + caveats in `DEPLOY.md` ("systemd socket activation"). |
| `ironcache.socket` | **Install-ready.** Socket-activation unit (#389) paired with the service; systemd holds the listen queue across an upgrade restart. |
| `install.sh` | **Dead scaffold, pending wiring.** Still carries `__VERSION__` / `OWNER/REPO` placeholders; no release step substitutes them, and it is NOT published as a release asset (it even fails closed on the unsubstituted placeholder). |
| `Formula/ironcache.rb` | **Dead scaffold, pending wiring.** Same unsubstituted placeholders, and it references `*-apple-darwin` tarballs that no workflow builds (`release.yml` builds Linux targets only). |
| `Dockerfile` | **Unused local-build variant.** The published images are built from the ROOT `Dockerfile` / `Dockerfile.console` by `.github/workflows/image.yml`; this copy is not in that pipeline and is not hadolint-gated. |

Artifact signing (minisign) and the *embedded* reproducible SBOM
(cargo-auditable) are owned by ADR-0020 and `docs/design/SUPPLY_CHAIN.md`; the
per-release CycloneDX SBOM export and the SHA256 attestation layer live in
`release.yml`.
