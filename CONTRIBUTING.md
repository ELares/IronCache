# Contributing to IronCache

Thanks for your interest in IronCache. This is a documentation-first project.
Right now it is in its **research and specification** phase: there is no
implementation code yet, by design. The backlog of design, research, and
decision issues is the specification, and the [vision EPIC (#1)](https://github.com/ELares/IronCache/issues/1)
is the index of everything. The fastest way to help today is to engage with the
research: challenge a prior-art claim, add a source, or sharpen a design
decision on its issue.

## How we work

- **Research before architecture, architecture before code.** We are deliberately
  not writing the cache engine yet. We are first gathering what the world already
  knows (Redis, Valkey, KeyDB, DragonflyDB, Memcached, Garnet, and the academic
  caching literature), recording it as version-pinned prior art, and turning it
  into a vetted specification.
- **Decisions live on issues.** Every design decision states the alternative it
  rejected and why, so disagreement is easy to ground. Challenge a decision on
  its issue before sending text or, later, code that contradicts it.
- **Prior-art claims are sourced and pinned.** If you assert that another system
  does X, cite a primary source (official docs, release notes, source code, or a
  paper), pin the version you read it against, and add it to
  [`docs/prior-art/claims.yaml`](docs/prior-art/claims.yaml).
- **Small, single-purpose PRs.** One concern per PR. A reviewer should be able to
  hold the whole change in their head. Split unrelated work into separate PRs.
- **Link the owning issue.** Use `refs #N` when a PR makes partial progress on an
  issue, and `Closes #N` only when the PR fully resolves it.

## The merge bar

Every pull request needs two things before it can merge:

1. **Green CI.** During the research phase the merge-blocking checks are
   documentation gates: relative and external link checks, and the prior-art
   claims check (`scripts/ci/check-prior-art-claims.sh`) that asserts every
   claim cited in prose exists in `docs/prior-art/claims.yaml` and that every id
   is unique. When implementation begins, the Rust engineering gates (rustfmt,
   clippy with pedantic lints denied, the test matrix, an MSRV build, a static
   musl build, supply-chain `cargo-deny`, SPDX headers, and an embedded SBOM)
   are added and become merge-blocking, matching the bar used by the sibling
   IronBus project.
2. **An independent review.** Green CI is necessary but not sufficient. A
   maintainer other than the author must review and approve before merge. CI
   being green is never on its own a reason to merge.

## The engineering bar (applies once code begins)

- **Edition 2021 or later, with a pinned MSRV.** Do not use language or
  standard-library features newer than the MSRV.
- **No panics in library paths.** No `unwrap`, `expect`, or `panic!` in library
  code. Return a typed error instead.
- **Typed errors.** Surface failures as typed error enums, never as stringly
  typed or swallowed errors.
- **Single static binary.** The shipping artifact is one static musl binary per
  architecture that is both the server and the CLI, with a kernel-only
  dependency and an embedded SBOM.

## Keep a Changelog

Every PR updates `CHANGELOG.md`. Add a terse bullet under the appropriate
heading (`Added`, `Changed`, `Fixed`, `Security`) in the `## [Unreleased]`
section, and reference the owning issue (`refs #N` or `#N`). The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Prose style

Do not use em dashes or en dashes anywhere in prose, code comments, or commit
messages. Use commas, periods, or a rephrase instead.

## Developer Certificate of Origin

IronCache uses the Developer Certificate of Origin (DCO) rather than a
contributor license agreement. By signing off on a commit you certify that you
wrote the change or otherwise have the right to submit it under the project's
`MIT OR Apache-2.0` license, per the
[Developer Certificate of Origin](https://developercertificate.org).

Add a sign-off trailer to every commit:

```
Signed-off-by: Your Name <you@example.com>
```

The simplest way is to pass `-s` (or `--signoff`) to `git commit`:

```sh
git commit -s -m "your message"
```

The name and email in the trailer must match the commit author. Copyright is
held collectively by "The IronCache Authors".

## License

By contributing, you agree that your contributions are dual-licensed under your
choice of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), matching the rest
of the project.
