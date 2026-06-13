# ADR-0017: Per-tenet acceptance targets and release gates

Status: Accepted
Issue: #157

## Context

Only Efficient was pinned to measurable targets (#7, ADR-0016); the other four
tenets lived as qualitative prose, so four of five tenets were unfalsifiable.
The charter (#2) ranks the tenets; this gives each a measurable definition of
done and a release gate, so "we shipped the tenet" is testable.

## Decision

Each tenet has a release gate, in charter rank order:

- **Compatible:** a stated Tier-0 differential-oracle pass rate against pinned
  Valkey [valkey-resp-identical] is a hard merge/release gate (#97); a tier is
  "met" only when its conformance suite (#95) is green (ADR-0009).
- **Efficient:** the ADR-0016 metrics with explicit numeric bars vs
  Valkey/Dragonfly (per-core throughput strictly above Valkey single-core;
  bytes-per-item below Redis at a fixed hit ratio), enforced by the regression
  gate (#159).
- **Simple:** a static-binary size ceiling, a kernel-only runtime-dependency
  count of zero beyond libc/the kernel (musl static build [rust-musl-crt-static-default]),
  and an install-to-first-GET time bound.
- **Scalable:** a per-core scaling-efficiency target out to N cores, plus the
  cluster budgets in ADR-0012 (#146).
- **AI-Driven:** the advisor must never regress below the static baseline
  (#90/#154) and the engine must be fully correct and fast with the advisor
  disabled (ADR-0013).

## Rejected Alternatives

- **Leave four tenets as prose (only Efficient measured).** Rejected: a ranked
  tenet with no test is a slogan; the charter's ordering only has teeth if each
  tenet can be shown met or not met.
- **One global pass/fail gate instead of per-tenet gates.** Rejected: the tenets
  trade off against each other in rank order, so they need separable gates to see
  which one a change regressed; a single gate hides that.

## Consequences

- Each gate is owned and implemented by its subsystem (Compatible by #95/#97,
  Efficient by #8/#159, Simple by the single-binary work #81/#84, Scalable by
  #146 and the cluster benches, AI-Driven by #90/#154).
- The exact numeric bars (size ceiling, install-time bound, scaling-efficiency
  target) are filled in as their owning issues land their measurements; this ADR
  fixes the gate structure and that each tenet must have one.
- Native metrics (unlike Redis [redis-no-builtin-prometheus]) feed the gates'
  measurements.
