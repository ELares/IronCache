# Design: Supply-chain audit gate (dependency vulnerabilities and licenses)

Issue: #144. Decisions: ADR-0020 (artifact signing + cargo-auditable SBOM),
ADR-0017 (Simple gate: minimal, auditable dependency surface). Related: #84
(packaging/distribution and the SBOM), #81 (single static binary), #54/ADR-0021
(the C-bound zstd dependency that makes license/advisory review concrete).

## Goal and scope

A from-scratch cache that takes dependencies must prove, on every merge and every
release, that it ships no known-vulnerable crate and no license it cannot honor.
This specifies that gate: a `cargo-deny` policy (advisories, licenses, bans,
sources) plus a `cargo-audit` / RustSec scan, wired as a merge and release block.
It completes the supply-chain loop ADR-0020 opened with signing and the SBOM. The
repo is docs-only today, so this specifies the policy and its enforcement
contract; the `deny.toml` and the CI job land with the first Cargo project and
activate the moment a `Cargo.lock` exists. The (future) Rust dependency list is
out of scope.

## Design

### cargo-deny: the four-check policy

- A committed `deny.toml` runs `cargo-deny`'s four checks
  [cargo-deny-four-checks]: **advisories** (deny-by-default against the RustSec
  database), **licenses** (an explicit SPDX allow-list), **bans** (forbidden
  crates and duplicate-version detection), and **sources** (only allow-listed
  registries/git sources). cargo-deny is pinned to a fixed version
  [cargo-deny-four-checks] so the policy result is reproducible.
- The **license allow-list is load-bearing**, not boilerplate: IronCache
  deliberately avoids SSPL/RSAL artifacts (it tracks Valkey, not relicensed
  Redis), and ADR-0021 links the C `zstd`/`zstd-sys` crates, so the allow-list
  must admit exactly the project's dual MIT-OR-Apache-2.0 posture plus the vetted
  licenses of the C-binding and allocator crates, and deny anything else by
  construction.

### cargo-audit and the RustSec advisory database

- `cargo-audit` scans `Cargo.lock` against the RustSec advisory database and
  reports each finding as crate name/version/advisory-id/severity
  [cargo-audit-rustsec-lockfile-scan]. The advisory database is the community
  RustSec one (advisory IDs `RUSTSEC-YYYY-NNNN`, maintained by the Rust Secure
  Code WG) [rustsec-advisory-db-wg-secure-code]. cargo-deny's advisories check and
  cargo-audit overlap deliberately: cargo-deny gives one policy file for the merge
  gate, cargo-audit gives a focused lockfile scan for scheduled/release runs and a
  second engine so a single tool's gap does not hide an advisory.

### Enforcement: merge gate and release gate

- On every PR the gate runs and **blocks merge** on any denied advisory, a
  disallowed license, a banned/duplicate crate, or a non-allow-listed source. On
  release it re-runs against the exact locked versions in the signed artifact
  (ADR-0020), so the SBOM and the advisory state are consistent at the moment of
  signing.
- **Exceptions are explicit and time-boxed**: an unavoidable advisory with no
  fixed version is recorded in `deny.toml` with the advisory id, a rationale, and
  an expiry, so a silent permanent ignore is impossible and the exception
  resurfaces as a failure when it lapses. There is no blanket allow.

### Activation in the docs-only phase

- The repo carries no `Cargo.lock` yet, so the CI job is authored to run the gate
  only once a Cargo project exists (a presence check on `Cargo.toml`/`Cargo.lock`)
  and is a no-op until then, rather than failing an empty workspace. The documented
  policy here is the deliverable now; the `deny.toml` and the CI job land with the
  first crate, matching the design-now / build-at-implementation milestone posture.

## Open questions

- The exact SPDX allow-list contents (finalized with the first real dependency
  set and the ADR-0021 C-binding license review).
- Whether cargo-deny alone suffices or cargo-audit runs as a scheduled second
  engine (cost vs redundancy), measured once the dependency tree exists.
- The exception expiry window (for example 30 vs 90 days) before a lapsed ignore
  re-blocks.

## Acceptance and test hooks

- A committed `deny.toml` encodes all four checks with advisories deny-by-default
  and an explicit SPDX allow-list [cargo-deny-four-checks]; CI runs it on every PR
  and blocks merge on any violation once a `Cargo.lock` exists.
- A seeded known-vulnerable crate in a test lockfile is caught by both cargo-deny
  advisories and cargo-audit against the RustSec database
  [cargo-audit-rustsec-lockfile-scan][rustsec-advisory-db-wg-secure-code].
- A disallowed license (for example an SSPL crate) fails the licenses check; a
  non-allow-listed registry source fails the sources check.
- A recorded exception carries an advisory id, rationale, and expiry, and the gate
  fails again when the expiry lapses.

## References

- ADR-0017, ADR-0020, ADR-0021; issues #84, #81, #54, #1; spec CLI_BINARY.md.
- Claims: [cargo-deny-four-checks], [cargo-audit-rustsec-lockfile-scan],
  [rustsec-advisory-db-wg-secure-code].
