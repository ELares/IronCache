<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Releasing IronCache

IronCache ships as a single static binary for four Linux targets, through two
release channels. Versioning and publishing are **fully automated**: a release
is cut on every push to `main`, with no human tagging required.

| Platform | Build target (internal) | Published asset |
| --- | --- | --- |
| x86_64, static | `x86_64-unknown-linux-musl` | `ironcache-linux-amd64-musl.tar.gz` |
| aarch64, static | `aarch64-unknown-linux-musl` | `ironcache-linux-arm64-musl.tar.gz` |
| x86_64, glibc >= 2.17 | `x86_64-unknown-linux-gnu.2.17` | `ironcache-linux-amd64-glibc.tar.gz` |
| aarch64, glibc >= 2.17 | `aarch64-unknown-linux-gnu.2.17` | `ironcache-linux-arm64-glibc.tar.gz` |

The published asset name uses a friendly CPU-arch (`amd64`/`arm64`) plus libc
flavor, dropping the Rust triple's `unknown` vendor field (which confuses
non-Rust operators), and carries no version, so
`https://github.com/ELares/IronCache/releases/latest/download/<asset>` is a
stable URL across builds (the version lives in the release tag and in
`ironcache --version`). The `musl` builds are fully static (no libc needed); the
`glibc` builds pin a glibc 2.17 floor for older distributions. Both carry an
embedded `cargo-auditable` SBOM.

## The two channels

### Rolling, calendar-versioned (the continuous channel)

Published **automatically on every push to `main`** by the `rolling-release`
workflow (`.github/workflows/rolling-release.yml`). The version is `YYYY.MMDD.N`:
the UTC date plus a per-day build number, e.g. `2026.0615.1` then `2026.0615.2`,
resetting the next day. `N` is `max(existing same-day build numbers) + 1`, which
is robust against deleted or skipped tags (counting is gap-fragile).

Each rolling build publishes the four reproducible tarballs, one consolidated
`SHA256SUMS`, and a keyless Sigstore build-provenance attestation, as a **normal**
release, so GitHub's `releases/latest` always points at the newest rolling build.

There is **no changelog gate** and no signing secret on this channel (rolling
builds are continuous, not curated). Skip a build by putting `[skip release]` in
the head commit message.

### Formal, tagged `v*` (the curated channel)

Cut by the maintainer by pushing a `v*` tag, built by the `release` workflow
(`.github/workflows/release.yml`). This curated channel adds:

- a **changelog gate** (`scripts/ci/changelog-unreleased.sh`) that FAILS the
  release before any binary is built if the CHANGELOG section is empty;
- a standalone **CycloneDX SBOM** (`ironcache.cyclonedx.json`) exported from the
  binary's embedded `cargo-auditable` data (`auditable2cdx`, #123);
- a **minisign** detached signature over `SHA256SUMS` (ADR-0020), when the
  `MINISIGN_SECRET_KEY` repository secret is provisioned (it warns and skips
  otherwise, and the keyless attestation still ships);
- the same keyless Sigstore build-provenance attestation as the rolling channel;
- prerelease marking for `v0.*` tags (not yet a stability promise).

Both channels reuse the same `cargo-zigbuild` reproducible cross-build setup the
CI workflow proves on every PR, so neither release runs unproven build steps.

## Cutting a formal release

1. Land all changes via the normal PR flow (CI green, reviewed, merged).
2. In a final PR, move the `## [Unreleased]` section of `CHANGELOG.md` under a
   new `## [X.Y.Z]` heading and (optionally) bump any human-facing version
   references. The workspace `Cargo.toml` version stays at `0.0.0` on purpose
   (see "Version stamping" below).
3. After it merges, tag the merge commit and push the tag:

   ```sh
   git tag -s vX.Y.Z -m "vX.Y.Z"   # signed (or -a for annotated)
   git push origin vX.Y.Z
   ```

   The `release` workflow can also be run from the Actions tab
   (`workflow_dispatch`). Re-running it for a tag that already has a published
   release fails at `gh release create` (it does not clobber); delete the
   existing release first (`gh release delete vX.Y.Z`) to rebuild it.

## Version stamping

The workspace `Cargo.toml` version is pinned at `0.0.0`, and `Cargo.lock` pins
every crate there too. The published version is stamped into the binary at build
time via the `IRONCACHE_BUILD_VERSION` environment variable, read by
`option_env!` in `cli::BUILD_VERSION`:

- the rolling channel sets it to the calendar version `YYYY.MMDD.N`;
- the formal channel sets it to the tag (minus the leading `v`);
- dev/CI/test builds leave it unset and fall back to `CARGO_PKG_VERSION`
  (`0.0.0`).

`option_env!` is read at compile time and never touches `Cargo.lock`, so it
cannot break `cargo build --locked` the way a `Cargo.toml` version bump would.
`crates/ironcache/build.rs` emits `rerun-if-env-changed=IRONCACHE_BUILD_VERSION`
so a warm `target/` re-stamps when the variable changes rather than baking a
stale value. `ironcache --version` reports the stamped value.

## Verifying a download

```sh
# Integrity: every asset's digest is pinned in SHA256SUMS.
sha256sum -c SHA256SUMS

# Provenance: the keyless Sigstore attestation, on both channels.
gh attestation verify <asset> --repo ELares/IronCache

# Signature (formal channel, when minisign signing is enabled):
minisign -Vm SHA256SUMS -P <ironcache-public-key>
```
