# Governance

IronCache is maintainer-led. A small group of maintainers stewards the project,
reviews and merges pull requests, and is responsible for keeping the design
coherent. Every change still follows the contribution process: green CI plus an
independent review before merge.

## How decisions are made

- **Decisions are recorded on their owning GitHub issues.** Each subsystem has a
  design issue, and the rationale for a decision, including the alternative it
  rejected, lives as a comment or update on that issue. The issues are the
  authoritative design record.
- **The README is the canonical vision.** The [README](README.md) states the
  product's tenets, scope, and committed non-goals. When in doubt about
  direction, the README is the reference.
- **Prior-art claims are version-pinned and machine-checked.** Every numeric or
  version-specific statement IronCache makes about another system lives in
  [`docs/prior-art/claims.yaml`](docs/prior-art/claims.yaml) with a source URL,
  the pinned upstream version, and a confidence level. That file is the single
  descriptive source of truth, and CI checks that the prose agrees with it.
- **Frozen design decisions win over stale text.** When a recorded, frozen
  decision conflicts with prose somewhere in the repository, the frozen decision
  is authoritative and the stale text is corrected to match it. A decision is
  changed by reopening it on its issue, not by quietly editing downstream text.

## Project phases

IronCache is documentation-first. The current phase is **research and
specification**: the design is being vetted in the GitHub issues before any
implementation code is written. The vision EPIC (issue #1) is the index of
everything. Implementation begins only after the architecture specification is
agreed, and then proceeds one small, reviewed, CI-gated pull request at a time.

## Maintainers

Maintainers carry the merge bit and the responsibility that comes with it.
Becoming a maintainer follows from a sustained record of well-reviewed
contributions and is decided by the existing maintainers. Copyright in the
project is held collectively by "The IronCache Authors".
