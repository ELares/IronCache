# Changelog

All notable changes to IronCache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will
adopt [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it ships a
release.

## [Unreleased]

### Added

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
