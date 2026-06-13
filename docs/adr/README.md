# Architecture Decision Records

Every load-bearing IronCache decision is recorded here as a numbered, immutable
Architecture Decision Record (ADR). This directory is governance, not design:
it owns the record format and the registers; the decisions themselves are made
on their `[DECISION]` issues and frozen here.

## Format

Each ADR is `NNNN-kebab-title.md` (zero-padded number) with exactly these four
sections, plus a one-line `Status:` and an `Issue:` back-link in the header:

```
# ADR-NNNN: Title

Status: Accepted        # Proposed | Accepted | Superseded
Issue: #N               # the [DECISION] issue this resolves

## Context
## Decision
## Rejected Alternatives
## Consequences
```

See [0000-template.md](0000-template.md). We reject the lighter "one row in a
decision-log table" format: per the tenets, a decision that does not name the
alternative it rejected and the evidence that settled it is not durable, and a
one-liner cannot carry that.

## Rules

1. **One ADR per `[DECISION]` issue**, linked both ways (the issue links the
   ADR; the ADR's `Issue:` header links the issue).
2. **Cite the evidence that settled it.** Where a decision turns on a prior-art
   fact, the ADR cites the claim id in square brackets, for example
   `[dragonfly-shard-formula]`, resolving to
   [`../prior-art/claims.yaml`](../prior-art/claims.yaml).
3. **Immutable after acceptance.** An accepted ADR is never edited in substance.
   A reversal is a new ADR carrying `Superseded-by: ADR-NNNN` in the old one and
   a `Supersedes: ADR-MMMM` note in the new one. `Status:` is exactly one of
   `Proposed`, `Accepted`, `Superseded`.
4. **Conflicts resolve by tenet order:** Compatible > Efficient > Simple >
   Scalable > AI-Driven (ratified in [ADR-0001](0001-adopt-adrs-and-tenet-conflict-order.md)).

## Registers

- [INDEX.md](INDEX.md): every ADR and the `[DECISION]` issue it resolves.
- [OPEN.md](OPEN.md): decisions not yet made, with owning area, the blocking
  research, target milestone, and a critical-path flag.
- [QUESTIONS.md](QUESTIONS.md): the research-question map, each open question
  from the research corpus pointing at the issue that resolves it.

## CI contract

[`../../scripts/ci/check-adr-index.sh`](../../scripts/ci/check-adr-index.sh)
runs in CI and is offline and deterministic. It fails when:

- an ADR record is missing one of the four required sections or a valid
  `Status:` line;
- an ADR cites a `[claim-id]` that is not present in `claims.yaml`;
- a `Superseded-by:` or `Supersedes:` link points at an ADR number with no file;
- an ADR file is not listed in `INDEX.md`.

Binding a *closed* `[DECISION]` issue to the existence of its ADR requires the
GitHub API and is therefore tracked as a separate (non-blocking) check rather
than gating every offline docs build; the offline gate above keeps the records
themselves honest.
