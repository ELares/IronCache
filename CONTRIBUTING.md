# Contributing to IronCache

Thanks for your interest in IronCache. IronCache is a Redis-compatible cache in a
single static Rust binary: thread-per-core, shared-nothing, with optional
replication and clustering. The engine is implemented and broad (see the
[README](README.md) for the feature set), exercised by 1,500+ in-tree tests, a
differential harness that proves RESP parity against a real `redis-server`, and a
real client-driver matrix. This document is everything you need to go from a fresh
clone to green tests and a mergeable pull request.

The design rationale lives in the [architecture decision records](docs/adr/), the
[subsystem design docs](docs/design/), and the
[GitHub issues](https://github.com/ELares/IronCache/issues) (indexed from the
[vision EPIC (#1)](https://github.com/ELares/IronCache/issues/1)). Read the ADR or
design doc a change touches before you write code that contradicts it.

## Quick start (clone to green tests)

You need Git and a working Rust install (`rustup` recommended). You do **not** need
to pick a toolchain: [`rust-toolchain.toml`](rust-toolchain.toml) pins the channel
(currently `1.92.0`, with `rustfmt` and `clippy`), and Cargo auto-installs and uses
it the first time you build. The workspace is edition 2024 with a Minimum Supported
Rust Version (MSRV) of **1.85**, so do not use language or standard-library features
newer than 1.85 (CI has a dedicated 1.85 build gate).

```sh
git clone https://github.com/ELares/IronCache
cd IronCache

cargo build --workspace          # first build also installs the pinned toolchain
cargo test  --workspace          # 1,500+ tests

# run just one crate's tests while iterating
cargo test -p ironcache-config
```

`cargo test` runs the standard test harness. CI runs `cargo test --workspace
--all-features` (see the gates below). `cargo nextest run` is a compatible, faster
local runner that some contributors use and that the project is adopting; either is
fine locally.

To boot the server and talk to it with any Redis client:

```sh
cargo run -p ironcache -- server
redis-cli -p 6379 SET hello world   # -> OK
redis-cli -p 6379 GET hello         # -> "world"
```

## Your first pull request

1. **Find a good first issue.** Browse the
   [`good first issue`](https://github.com/ELares/IronCache/labels/good%20first%20issue)
   and [`help wanted`](https://github.com/ELares/IronCache/labels/help%20wanted)
   labels (or `gh issue list --label "good first issue"`). Comment on the issue so
   work is not duplicated.
2. **Branch, then build and test** with the Quick start above.
3. **Make a small, single-purpose change.** One concern per PR. A reviewer should be
   able to hold the whole change in their head; split unrelated work into separate
   PRs.
4. **Run the local pre-flight** (next section) so CI is green on the first push.
5. **Update `CHANGELOG.md`.** Add a terse bullet under the right sub-heading
   (`### Added`, `### Changed`, `### Fixed`, `### Security`) in the `## [Unreleased]`
   section, referencing the issue (`#N`). The format follows
   [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
6. **Open the PR.** Use `refs #N` for partial progress on an issue and `Closes #N`
   only when the PR fully resolves it. Every commit is signed off (see
   [Developer Certificate of Origin](#developer-certificate-of-origin)).

### Local pre-flight (mirror the CI gates)

Run these before you push and they will match what CI enforces. CI sets
`RUSTFLAGS="-D warnings"`, so any warning is a failure; the commands below reproduce
that.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
bash scripts/ci/check-rust-invariants.sh
```

Optionally, to match the supply-chain and static-build gates:

```sh
cargo deny check advisories licenses bans sources          # needs cargo-deny
rustup target add x86_64-unknown-linux-musl && \
  cargo build --release --target x86_64-unknown-linux-musl # Linux hosts
```

## The merge bar

Every pull request needs two things before it can merge:

1. **Green CI.** All merge-blocking checks pass.
2. **An independent review.** Green CI is necessary but never sufficient. A
   maintainer other than the author must review and approve before merge.

## The CI gates

### `rust.yml` (the core engineering gates)

A leading `guard` job short-circuits the workflow while the repo is docs-only; now
that crates exist it is active. Each job below is merge-blocking:

| Job | What it runs | Enforces |
| --- | --- | --- |
| `fmt` | `cargo fmt --all --check` | rustfmt formatting |
| `clippy (pedantic, -D warnings)` | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | pedantic clippy with warnings denied (the pedantic set and its justified relaxations live in the workspace `Cargo.toml` `[workspace.lints.clippy]`) |
| `test (ubuntu-latest)` | `cargo test --workspace --all-features` | the test suite |
| `msrv (1.85)` | `cargo +1.85.0 build --workspace --all-features` | the code compiles on the declared MSRV floor |
| `musl static build` | `cargo build --release --target x86_64-unknown-linux-musl` | the shipping single static musl binary builds |
| `io_uring datapath` | clippy + build + test of `ironcache-runtime` / `ironcache` with `--features io_uring` | the Linux-only, default-off io_uring path stays lint-clean, builds, and passes its round-trip test |
| `invariant lints` | `bash scripts/ci/check-rust-invariants.sh` | the load-bearing invariants (see below) |
| `cargo-deny` | `cargo deny check advisories licenses bans sources` (policy in [`deny.toml`](deny.toml)) | supply chain: advisories, license allowlist, banned crates, source allowlist |

CI runs on Linux only: Linux is the sole supported deployment OS (the io_uring
datapath is Linux-only and the release images are Linux). Local macOS development
still works through the portable tokio path; it is just not a merge gate.

### Other workflows

- **`differential.yml`** boots the real IronCache server and a real `redis-server`
  side by side, replays a deterministic command corpus against both, and asserts the
  RESP2 replies match (with a documented allowlist for correct-by-design
  differences). It installs `redis-server` and runs `cargo test -p ironcache --test
  differential`. The differential test skips itself cleanly when `redis-server` is
  absent, so plain `cargo test --workspace` stays green on a machine without redis;
  this workflow is the one place that installs it and runs the test explicitly.
- **`driver-matrix.yml`** runs real Redis client libraries (redis-py, go-redis,
  ioredis) against both a single node and a turnkey 3-node Raft cluster via
  `tests/drivers/run.sh`, checking core ops, pipelining, MULTI/EXEC, pub/sub, RESP3,
  and cluster topology discovery (`CLUSTER SLOTS`) plus `MOVED`-routing end to end.
- **`docs.yml`** runs two offline, deterministic doc gates that are merge-blocking:
  the prior-art claims check (`scripts/ci/check-prior-art-claims.sh`, that every
  claim cited in prose exists in [`docs/prior-art/claims.yaml`](docs/prior-art/claims.yaml))
  and the **ADR index lint** (`scripts/ci/check-adr-index.sh`, see
  [ADR governance](#adr-and-design-doc-governance)).
- **`adr-governance.yml`** is advisory and **non-blocking**. On a weekly schedule,
  on demand, and on PRs that touch the ADR binding files, it reconciles closed
  `decision-needed` issues against ADR `Issue:` headers and reports mismatches to the
  run summary. It never fails the build.

Additional workflows exist for release, container images, and performance
(`release.yml`, `rolling-release.yml`, `image.yml`, `perf-gate.yml`, and others);
they are not the per-PR code gates and you normally do not need to think about them.

## Determinism: the rule every contributor must know

IronCache is designed for Deterministic Simulation Testing (DST): a seeded replay of
the same input must produce byte-identical eviction and expiry decisions. That is
only possible if nondeterminism cannot leak into the engine. Per
[ADR-0003](docs/adr/0003-design-for-determinism.md) and invariant 2 in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md):

- **No code on a decision path may call the OS clock, RNG, network, or disk
  directly.** It reaches them only through the `ironcache-env` seam (`Env`), which is
  the single controllable integration point for the simulated clock, fault injector,
  and deterministic scheduler.

The `invariant lints` CI gate (`scripts/ci/check-rust-invariants.sh`) enforces the
mechanical half of this and a few sibling invariants. It is a grep-based, offline
check that fails when it finds:

- a `fork()` call or libc fork binding anywhere (invariant 4, no-fork);
- direct real-time or entropy APIs (`std::time::Instant`/`SystemTime`,
  `Instant::now`, `SystemTime::now`, or the `chrono` / `time` / `fastrand` / `rand` /
  `quanta` / `coarsetime` / `minstant` / `web_time` / `getrandom` crates) **outside**
  the two sanctioned seams, `ironcache-env` (clock/RNG) and `ironcache-runtime` (the
  I/O and timer seam);
- `std::sync::Mutex` / `RwLock` in a hot-path (per-shard) crate (invariant 1,
  shared-nothing, ADR-0002);
- any `.rs` file missing the `SPDX-License-Identifier: MIT OR Apache-2.0` header.

Every new `.rs` file must start with that SPDX header. If you genuinely need one of
the guarded APIs at a sanctioned boundary, the script honors narrow, justified escape
comments (`// lint-allow: env-seam`, `// lint-allow: shared-nothing`,
`// lint-allow: fork-mention`); use them only with a reason a reviewer will accept.

Beyond the lints, keep to the engineering norms the codebase already follows: prefer
typed error enums over stringly-typed or swallowed errors, and avoid panics
(`unwrap` / `expect` / `panic!`) on non-test library paths.

## The Linux docker dev loop (io_uring on a non-Linux host)

The optional io_uring datapath ([`docs/design/IOURING_DATAPATH.md`](docs/design/IOURING_DATAPATH.md),
feature `io_uring`, default off) is `#[cfg(all(target_os = "linux", feature =
"io_uring"))]`, so it is inert on macOS/Windows and cannot be built or tested there
natively. To work on it from a non-Linux machine, build and test inside a Linux
container. Any Docker-compatible engine with a Linux VM works (Docker Desktop,
colima, and similar).

The pattern: mount the repo read-only, and back the Cargo home and target directory
with named volumes so the registry and build artifacts persist across runs (rebuilds
stay incremental).

```sh
# Build the Linux-only io_uring path (mirrors the CI io_uring job).
docker run --rm \
  -v "$PWD:/repo:ro" \
  -v ic-cargo:/cargo -v ic-target:/target \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target \
  -w /repo rust:1-bookworm \
  bash -c 'cargo build -p ironcache-runtime -p ironcache --features io_uring'

# Run the io_uring runtime tests (the round-trip needs a kernel with io_uring,
# which the container's Linux VM provides).
docker run --rm \
  -v "$PWD:/repo:ro" \
  -v ic-cargo:/cargo -v ic-target:/target \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target \
  -w /repo rust:1-bookworm \
  bash -c 'cargo test -p ironcache-runtime --features io_uring'
```

The `rust:1-bookworm` image already carries a toolchain that satisfies
`rust-toolchain.toml`; the pinned channel is auto-installed on first use inside the
container if needed. The same container is a convenient way for a macOS contributor
to reproduce any Linux-only CI result locally.

## ADR and design-doc governance

Load-bearing decisions are recorded as numbered, immutable Architecture Decision
Records under [`docs/adr/`](docs/adr/). Each is `NNNN-kebab-title.md` with a `Status:`
line, an `Issue:` back-link, and exactly four sections: `## Context`,
`## Decision`, `## Rejected Alternatives`, `## Consequences` (see
[`docs/adr/0000-template.md`](docs/adr/0000-template.md) and
[`docs/adr/README.md`](docs/adr/README.md)). Subsystem design docs live in
[`docs/design/`](docs/design/).

The **ADR index lint** (`scripts/ci/check-adr-index.sh`, run by `docs.yml`) is an
offline, deterministic hard gate. It fails when an ADR is missing one of the four
sections or a valid `Status:`, cites a `[claim-id]` absent from
[`claims.yaml`](docs/prior-art/claims.yaml), has a dangling `Superseded-by:` /
`Supersedes:` link, or is not listed in [`docs/adr/INDEX.md`](docs/adr/INDEX.md).

To propose an ADR:

1. The decision is discussed on its `[DECISION]` GitHub issue (that is the
   authoritative record; the ADR freezes the outcome).
2. Copy `0000-template.md` to the next free number, fill the four sections, and cite
   the evidence that settled it (`[claim-id]` references resolving to `claims.yaml`).
3. Add the file to `docs/adr/INDEX.md` and link the issue both ways (the issue links
   the ADR, the ADR's `Issue:` header links the issue).
4. An accepted ADR is never edited in substance. A reversal is a new ADR that adds
   `Superseded-by: ADR-NNNN` to the old record and `Supersedes: ADR-MMMM` to the new
   one.

Conflicts resolve by tenet order: Compatible > Efficient > Simple > Scalable >
AI-Driven (ADR-0001).

## How we work

- **Small, single-purpose PRs.** One concern per PR; split unrelated work.
- **Link the owning issue.** `refs #N` for partial progress, `Closes #N` for a full
  resolution.
- **Frozen decisions win over stale text.** If a change conflicts with a recorded
  decision, the decision is authoritative; change the decision on its issue, not by
  quietly editing downstream text.
- **Prior-art claims are sourced and version-pinned.** If you assert that another
  system does X, cite a primary source, pin the version you read it against, and add
  it to [`docs/prior-art/claims.yaml`](docs/prior-art/claims.yaml) (CI checks that
  prose agrees with that file).

## Prose style

Do not use em dashes or en dashes anywhere in prose, code comments, or commit
messages. Use commas, periods, `--`, or a rephrase instead.

## Developer Certificate of Origin

IronCache uses the Developer Certificate of Origin (DCO) rather than a contributor
license agreement. By signing off on a commit you certify that you wrote the change
or otherwise have the right to submit it under the project's `MIT OR Apache-2.0`
license, per the [Developer Certificate of Origin](https://developercertificate.org).

Add a sign-off trailer to every commit. The simplest way is `-s` (or `--signoff`):

```sh
git commit -s -m "your message"
```

This appends:

```
Signed-off-by: Your Name <you@example.com>
```

The name and email in the trailer must match the commit author. Copyright is held
collectively by "The IronCache Authors".

## License

By contributing, you agree that your contributions are dual-licensed under your
choice of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), matching the rest of
the project.
