# ADR-0001: Adopt ADRs and the ranked-tenet conflict order

Status: Accepted
Issue: #4

## Context

IronCache is a documentation-first project with a large, version-pinned
prior-art corpus and a five-tenet charter. Dozens of `[DECISION]` issues will
be settled before and during implementation, and many of them gate others (the
concurrency model shapes the allocator, which shapes the persistence stance).
A decision trail that lives only in issue comments rots: the rejected
alternative and the evidence that settled it get lost, and reversals become
invisible. We need a durable, uniform, machine-checkable record, and a single
rule for resolving conflicts between decisions.

## Decision

1. Every load-bearing decision is recorded as a numbered ADR under `docs/adr/`
   in the four-section format defined in [README.md](README.md), one ADR per
   `[DECISION]` issue, immutable after acceptance, superseded only by a newer
   ADR.
2. When two decisions or designs conflict, they are resolved strictly in tenet
   order: **Compatible > Efficient > Simple > Scalable > AI-Driven.** The higher
   tenet wins. This order is the project's single tie-break rule and every ADR
   may invoke it by name.

## Rejected Alternatives

- **A lightweight decision-log table (one row per decision).** Rejected: it
  cannot carry the rejected alternative and the settling evidence, so it is not
  durable. The four-section ADR is the minimum that records why.
- **No fixed tenet order, resolve conflicts case by case.** Rejected: ad hoc
  resolution reopens settled trade-offs endlessly. A fixed order makes
  conflicts decidable. Compatible outranks Efficient so a faster encoding that
  changes an observable reply loses; Efficient outranks Simple so a per-core win
  ships behind a safe-default knob; AI-Driven is last so a deterministic
  primitive always beats a learned heuristic.
- **Treating per-core throughput and fat-box aggregate QPS as equal headline
  numbers.** Rejected under Efficient: aggregate-QPS comparisons mislead, as in
  the marketing framing that pits many threads against few [dragonfly-25x-thread-asymmetry];
  at a single core the shared-nothing leader is only at parity with Redis
  [dragonfly-single-core-parity], so per-core is the honest bar.

## Consequences

- All subsequent `[DECISION]` issues close with a matching ADR; the offline
  `check-adr-index.sh` gate keeps records well-formed and their claim citations
  valid against `claims.yaml`.
- `INDEX.md`, `OPEN.md`, and `QUESTIONS.md` become the live decision registers.
- Reviewers cite the tenet order by name to settle disagreements, and ADRs name
  the tenet they invoke, so trade-offs are auditable rather than relitigated.
- This ADR is process-level and binds the project regardless of language or
  runtime choices made later.
