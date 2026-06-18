# Changelog

All notable changes to IronCache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will
adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it ships a
release.

## [Unreleased]

### Added

- The WRITE-SIDE replication guardrail (`min-replicas-to-write` /
  `min-replicas-max-lag`, ADR-0026): an owner REJECTS a write to a slot it owns
  with `-NOREPLICAS Not enough good replicas to write.` when fewer than
  `min_replicas_to_write` replicas are currently in sync (lag within
  `min_replicas_max_lag`), so an acknowledged write is known to be on at least
  that many replicas, bounding the failover loss window (the read side was
  already bounded by `replica_max_lag`). The primary's per-replica serve tasks
  maintain a node-level in-sync count as a single lock-free `AtomicUsize` (one
  relaxed load on the write path); the check is owned-write-only,
  cluster/raft-mode-only, and DISABLED by default (`min_replicas_to_write = 0`,
  the Redis default), so the write hot path is byte-unchanged at the default.
  Configurable via TOML / `IRONCACHE_MIN_REPLICAS_TO_WRITE` /
  `IRONCACHE_MIN_REPLICAS_MAX_LAG`.
- Fully automated versioning and releases, in two channels (RELEASING.md). A new
  `rolling-release` workflow publishes a calendar-versioned (`YYYY.MMDD.N`)
  GitHub Release on every push to `main`: four reproducible `cargo-zigbuild`
  tarballs (x86_64/aarch64, musl/glibc-2.17), a consolidated `SHA256SUMS`, and a
  keyless Sigstore build-provenance attestation, as a normal release so
  `releases/latest` tracks the newest build (`[skip release]` opts out). The
  published version is stamped into the binary via `IRONCACHE_BUILD_VERSION`
  (read by `option_env!` in `cli::BUILD_VERSION`, with a `build.rs`
  `rerun-if-env-changed` so a warm target re-stamps), so `ironcache --version`
  reports the build without touching `Cargo.lock` (pinned at `0.0.0`).
- The formal `v*` `release` workflow is now a working pipeline rather than a
  scaffold: a changelog gate (`scripts/ci/changelog-unreleased.sh`) that fails
  an empty-changelog release before building, a real CycloneDX SBOM export from
  the embedded `cargo-auditable` data (`auditable2cdx`, #123), a secret-gated
  minisign signature over `SHA256SUMS` (ADR-0020), a keyless Sigstore
  attestation, and `v0.*` prerelease marking. Fixed a `SHA256SUMS` bug where a
  `merge-multiple` artifact-download filename collision left three of the four
  tarballs unchecksummed; the consolidated file is now rebuilt from the tarballs
  and self-checked.
- First engine code (PR-1 "Boot + wire"): a Cargo virtual workspace and a
  minimal-but-real server that accepts RESP connections and answers the Tier-0
  connection commands. Seven crates: `ironcache-env` (the determinism Env seam,
  ADR-0003), `ironcache-protocol` (RESP2/RESP3 codec and the verbatim error
  catalog, PROTOCOL.md/ERRORS.md/ADR-0019), `ironcache-runtime` (the swappable
  Runtime trait and a shared-nothing tokio current-thread, per-core-pinned
  bootstrap with SO_REUSEPORT per-shard accept, RUNTIME.md/ADR-0002),
  `ironcache-config` (layered TOML config with human-size parsing, CONFIG.md),
  `ironcache-observe` (INFO sections and per-shard rollup counters,
  OBSERVABILITY.md), `ironcache-server` (the HELLO-driven connection state
  machine and Tier-0 dispatch: PING/HELLO/AUTH/SELECT/QUIT/RESET/CLIENT/COMMAND/
  INFO/ECHO), and the `ironcache` binary (clap subcommands server|cli|bench|
  check|config|upgrade, the redis-cli argv[0] alias, jemalloc global allocator
  per ADR-0006, graceful SIGINT/SIGTERM shutdown).
- Rust CI workflow (`.github/workflows/rust.yml`) with the docs-only guard idiom:
  fmt, clippy (pedantic, -D warnings), test (ubuntu + macos), MSRV 1.85, a musl
  static build, and grep-based invariant lints
  (`scripts/ci/check-rust-invariants.sh`) enforcing no-fork, the determinism
  Env-seam boundary, shared-nothing locks, and the SPDX header on every source.
- `deny.toml` (cargo-deny license allowlist, advisories, sources),
  `rust-toolchain.toml` (pinned stable channel), and a committed `Cargo.lock`.
- Project scaffolding: dual `MIT OR Apache-2.0` license, Code of Conduct,
  Contributing guide, Governance, and Security policy.
- The vision EPIC and the research and specification issue tree.
- Prior-art research corpus under `docs/research/`, the version-pinned
  `docs/prior-art/claims.yaml`, and the `docs/PRIOR_ART.md` survey.
- Pre-implementation audit (`docs/AUDIT.md`): every issue audited and given a
  verdict comment, 5 prior-art claims re-verified and corrected, 27 split
  sub-issues filed from 9 decomposed parents, and 36 coverage-gap issues filed.

### Changed

- Corrected 5 prior-art claims in `docs/prior-art/claims.yaml` after
  re-verification (provenance preserved via `verification.reaudited`).

### Fixed

- Removed or relinked broken citations in issue bodies (#83, #88, #97).
