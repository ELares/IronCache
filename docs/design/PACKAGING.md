# Design: Packaging, cross-build matrix, install paths, and per-release attestation

Issue: #84. Decisions: ADR-0020 (minisign signing + cargo-auditable SBOM),
ADR-0017 (Simple gate: static musl, kernel-only runtime, install-to-first-GET).
Related: #121 (cross-build matrix), #122 (install paths + hardened unit), #123
(SBOM + attestation), #125 (Windows non-goal), #81 (single static binary), #83
(upgrade), #144 (supply-chain gate), #41/#42 (allocator bake-off), #1 (vision).

## Goal and scope

IronCache ships as exactly one reproducible static binary per architecture
(CLI_BINARY.md), and this spec is where that binary becomes trivially
installable, byte-for-byte verifiable, and tiny on the wire. It pins four
things: a reproducible cross-build matrix (#121), the checksum-validated install
paths plus a hardened systemd unit (#122), per-release SHA256 attestation plus
the per-release CycloneDX SBOM export over every artifact (#123), and the
recorded Windows non-goal (#125). It closes the #84 umbrella by gathering those
four children into one contract.

This spec deliberately does NOT re-specify what neighbors already own. Artifact
signing (minisign) and the embedded reproducible SBOM (cargo-auditable, sorted
and timestamp-free) are settled by ADR-0020 and SUPPLY_CHAIN.md; here they are
referenced. On top of them this spec adds two things ADR-0020 / SUPPLY_CHAIN.md
do not specify: the per-release CycloneDX SBOM export (derived from the embedded
cargo-auditable data via auditable2cdx) and the SHA256 attestation layer. The
dependency-advisory/license merge-and-release gate is SUPPLY_CHAIN.md (#144).
The musl-malloc versus statically-linked-mimalloc/jemalloc contention benchmark
that decides whether musl is the performance default or only the portable
fallback lives in `docs/experiments/allocator-bakeoff.md` under #41/#42; this
spec consumes its verdict but does not run it. Subcommand dispatch and the
`upgrade` flow are CLI_BINARY.md / ADR-0020 / #83. The repo is docs-only today,
so the deliverable now is this spec plus templated, inert scaffold artifacts
under `packaging/` and a release workflow that activates on the first tagged
release.

## Design

### Cross-build matrix and the reproducibility contract (#121)

One host builds the entire matrix with cargo-zigbuild
[cargo-zigbuild-version-features], which uses Zig as the linker and C toolchain
so there is no Docker-image-per-target zoo to maintain (the reason
cargo-zigbuild is chosen over `cross`). The matrix is:

- `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`: fully static,
  kernel-only binaries. `crt-static` is on by default for the x86_64 musl target
  and is set explicitly on aarch64 musl, so libc is linked into the binary and
  there is zero glibc runtime dependency [rust-musl-crt-static-default]. These
  are the primary artifacts and satisfy the Simple-gate kernel-only-runtime
  requirement (ADR-0017).
- A glibc-version-pinned `*-linux-gnu` fallback: cargo-zigbuild pins a minimum
  glibc per target via the target suffix (for example
  `aarch64-unknown-linux-gnu.2.17`) [cargo-zigbuild-version-features], so the
  fallback runs on old enterprise glibc without dynamically requiring a newer
  one. This is the escape hatch for any environment where the musl build is the
  performance fallback rather than the default (the allocator verdict from
  #41/#42).
- macOS is shipped via the Homebrew formula below; cargo-zigbuild can also build
  the macOS targets from the same host [cargo-zigbuild-version-features].

The reproducibility contract: the committed, pinned `Cargo.lock` is the input
base; the build sets `SOURCE_DATE_EPOCH`, a fixed `--remap-path-prefix`, and a
fixed target/toolchain so a re-run on the pinned lockfile yields byte-identical
artifact digests. The embedded SBOM does not perturb this because cargo-auditable
is itself reproducible-build safe (no timestamps, sorted JSON)
[cargo-auditable-version-reproducible]. Acceptance is a digest-equality check
between two independent builds of the same tag.

### Install paths, all checksum-validated (#122)

Every path verifies a SHA256 before it trusts a byte.

- **Owned, pinned `curl | sh` installer.** A `packaging/install.sh` that we own
  and pin in-repo (not a third-party hosted script), invoked as
  `curl --proto '=https' --tlsv1.2 -LsSf <url>/install.sh | sh`. It detects
  OS/arch, downloads the matching artifact, validates the embedded SHA256 before
  unpacking, and manages PATH with an opt-out env var
  (`IRONCACHE_NO_MODIFY_PATH`). This mirrors the cargo-dist installer contract
  [cargo-dist-installer-curl-sh] but the script is vendored so the supply chain
  is ours end to end.
- **Plain Homebrew formula.** `brew install ironcache` via a single core-style
  formula: no tap, no cask, no conflicting formula. That is strictly easier than
  Redis or Valkey on macOS, where the two upstream formulae mutually conflict
  [valkey-brew-plain-formula]. The formula verifies the bottle/source SHA256.
- **Distroless-static / scratch image under 5 MiB.** A multi-stage build copying
  the static musl binary onto `gcr.io/distroless/static` (about 2 MiB, with CA
  certs and a nonroot user) [distroless-static-size]; scratch is the
  absolute-minimum alternative. Either keeps the published image under the 5 MiB
  ceiling and runs as nonroot.
- **Hardened systemd unit.** `packaging/ironcache.service` runs the daemon under
  a defense-in-depth sandbox. Eight directives carry the load:
  - `NoNewPrivileges=yes` blocks privilege gain through execve (setuid/setgid
    and file capabilities are neutralized) [systemd-nonewprivileges-blocks-execve-priv-gain].
  - `ProtectSystem=strict` makes the entire filesystem read-only except
    explicitly allowed paths [systemd-protectsystem-strict-readonly-fs].
  - `ProtectHome=yes` makes `/home`, `/root`, and `/run/user` inaccessible
    [systemd-protecthome-inaccessible-home-root-runuser].
  - `PrivateTmp=yes` gives the service a namespaced private `/tmp` and
    `/var/tmp` [systemd-privatetmp-namespaced-tmp].
  - `DynamicUser=yes` runs under a transient, allocated-per-start UID/GID with
    no persistent account [systemd-dynamicuser-transient-uid-gid].
  - `RestrictAddressFamilies=` limits the socket address families the service
    may open [systemd-restrictaddressfamilies-limits-sockets].
  - `MemoryDenyWriteExecute=yes` forbids writable-and-executable memory
    mappings (W^X) [systemd-memorydenywriteexecute-blocks-w-x-mappings].
  - `CapabilityBoundingSet=` drops the capability bounding set to empty so the
    process can hold no privileged capability [systemd-capabilityboundingset-limits-caps].
  Persisted-data services set `ReadWritePaths=` for the state directory under
  the strict read-only filesystem; the in-memory default ships with none.

### Per-release SBOM export and attestation (#123)

Every artifact (each target's archive, the installer, the image manifest, the
CycloneDX SBOM) gets a published `SHA256SUMS` file as part of the GitHub
release, and each install path verifies against it. This spec adds two release
layers on top of what ADR-0020 / SUPPLY_CHAIN.md already own. First, the
per-release CycloneDX SBOM export: the embedded cargo-auditable bill of
materials [cargo-auditable-version-reproducible] is converted to a standalone
CycloneDX artifact (via auditable2cdx) and published with the release; ADR-0020
owns the *embedded* SBOM, this spec owns the *exported* CycloneDX file. Second,
the SHA256 attestation layer, distinct from the minisign detached signature
already owned by ADR-0020: minisign proves authorship offline with one small
public key (no PKI), cargo-auditable proves the bill of materials, and the
SHA256SUMS proves byte integrity. `upgrade` (CLI_BINARY.md, #83) checks the
minisign signature; the installer and brew check the SHA256.

### Non-goal: native Windows server binary (#125)

A native Windows server binary is a deferred M2 non-goal. Like Redis and
Dragonfly, IronCache punts Windows to Docker or WSL and ships Linux musl plus
macOS first. The distroless image above is the supported Windows path (run under
Docker Desktop / WSL2); no `*-pc-windows-*` target is in the cross-build matrix
for M2. This is a scope decision, recorded so it is falsifiable rather than
forgotten; revisiting it is out of scope for #84.

### Scaffold posture

The artifacts this spec specifies ship now as templated, inert scaffolds:
`packaging/install.sh`, `packaging/Formula/ironcache.rb`, `packaging/Dockerfile`,
`packaging/ironcache.service`, and `.github/workflows/release.yml`. They carry
placeholders (`__VERSION__`, `__SHA256_*__`, the repo slug) that the release job
fills at the first tag, and the workflow triggers only on `v*` tags and
short-circuits while the repo has no `Cargo.toml`. This is the same design-now /
build-at-implementation posture SUPPLY_CHAIN.md uses for `deny.toml`.

## Open questions

- The exact static-binary size ceiling (the Simple-gate bar, ADR-0017) and
  whether `bench`/`check` ship always or behind a build feature to stay under
  it (CLI_BINARY.md open question, interacts with the under-5-MiB image).
- Whether musl is the performance default or the portable fallback to the
  glibc-pinned gnu build, resolved by the #41/#42 allocator contention
  benchmark, not here.
- The minimum glibc version to pin on the gnu fallback target (oldest
  enterprise LTS still in support versus build-host availability).
- Whether the container base is `distroless/static` (CA certs + nonroot, about
  2 MiB) or `scratch` (absolute minimum, no certs), decided once TLS-to-origin
  needs of `upgrade` are fixed.

## Acceptance and test hooks

- CI builds `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, and the
  glibc-version-pinned `*-linux-gnu` fallback on cargo-zigbuild from the pinned
  `Cargo.lock`; the musl artifacts are confirmed kernel-only (no dynamic libc
  dependency).
- Two independent builds of the same tag produce byte-identical artifact
  digests (reproducible from the pinned `Cargo.lock`), with the embedded SBOM
  present and unchanged across the two builds.
- The vendored `curl | sh` installer validates the SHA256 before unpacking,
  honors `IRONCACHE_NO_MODIFY_PATH`, and installs a runnable binary on x86_64
  and aarch64 Linux.
- `brew install ironcache` succeeds via a plain formula with no tap, no cask,
  and no conflict, and the formula's SHA256 matches the release `SHA256SUMS`.
- The published container image is under 5 MiB, runs as nonroot, and serves
  `PING`.
- `systemd-analyze security ironcache.service` reports a hardened exposure
  level with all eight directives above in force; the unit starts the daemon
  under a transient user with a read-only root filesystem.
- The release publishes a `SHA256SUMS` covering every artifact, the per-release
  CycloneDX SBOM exported from the embedded cargo-auditable data, plus the
  minisign signatures owned by ADR-0020 / SUPPLY_CHAIN.md.
- A doc/CI assertion records the native Windows server binary as a deferred M2
  non-goal (Docker/WSL) and that no Windows target is in the matrix.

## References

- ADR-0017, ADR-0020, ADR-0021; issues #84, #121, #122, #123, #125, #81, #83,
  #144, #41, #42, #8, #1; specs CLI_BINARY.md, SUPPLY_CHAIN.md;
  experiment `docs/experiments/allocator-bakeoff.md`.
- Claims: [cargo-zigbuild-version-features], [rust-musl-crt-static-default],
  [cargo-auditable-version-reproducible], [cargo-dist-installer-curl-sh],
  [valkey-brew-plain-formula], [distroless-static-size],
  [systemd-nonewprivileges-blocks-execve-priv-gain],
  [systemd-protectsystem-strict-readonly-fs],
  [systemd-protecthome-inaccessible-home-root-runuser],
  [systemd-privatetmp-namespaced-tmp],
  [systemd-dynamicuser-transient-uid-gid],
  [systemd-restrictaddressfamilies-limits-sockets],
  [systemd-memorydenywriteexecute-blocks-w-x-mappings],
  [systemd-capabilityboundingset-limits-caps].
