# AI-assisted design pipeline

IronCache is designed by an LLM agent pipeline, not authored freehand. This
runbook documents that pipeline: the loop that mined the prior art into
[`prior-art/claims.yaml`](prior-art/claims.yaml) and produced the
pre-implementation [`AUDIT.md`](AUDIT.md), and the gates that keep every numeric
or version-specific assertion in the design tree sourced, unique, and
human-approved.

This is a process and governance document. It sits beside the
[charter](CHARTER.md) and the [ADR governance](adr/README.md), not under
`docs/design/`: it describes how the design is produced, not a subsystem to be
implemented. It realizes the design ratified in #94 (decomposed from #88), the
AI-driven concern of which is tracked as #127.

## Why a pipeline, not freehand authoring

Cache design lives or dies on numbers: hit ratio, tail latency, bytes saved,
eviction quality. Agent-proposed mechanisms tend to cite numbers that are
plausible but unreproduced, source-free, or pinned to a workload that is not
ours. The pipeline treats each numeric claim as a hypothesis to be falsified,
ties it to a version-pinned source, and refuses to admit it to the design tree
until it survives an adversarial re-check and an offline gate. The reproduction
discipline is borrowed directly from the load-aware-caching literature: a
mechanism enters only after reproduced measurement on independent traces
[lrb-model-and-traffic-reduction]. The broader ML-for-caching framing
([lecar-regret-min-18x], [cacheus-experts], [wtinylfu-caffeine-sketch]) is
adapted, not borrowed wholesale: agent proposals may cite it, but every borrowed
number is re-derived against our own fixtures before it counts (see
"Harness-blocked: the trace-replay reproduction bar").

Per the tenet order (Compatible > Efficient > Simple > Scalable > AI-Driven),
this pipeline is dev-time infrastructure. It is independent of the runtime
advisor (#88's AI-Driven engine feature): no model runs on the request path, and
nothing here ships in the binary.

## The loop

```
  prior-art questions
        |
        v
  [1] agent fan-out mining ........ one agent per source/dimension
        |                            -> draft claims with version-pinned sources
        v
  [2] adversarial verifier ........ independent, refute-by-default
        |                            -> re-checks load-bearing claims vs primary
        |                               sources; verdict per claim
        v
     claims.yaml (descriptive source of truth, per-claim verification block)
        |
        v
  [3] offline citation/uniqueness gate ... scripts/ci/check-prior-art-claims.sh
        |                                   (already live, hard gate in CI)
        v
  [4] human PR review ............. final authority; no agent auto-merge path
```

### 1. Agent fan-out mining

Research agents fan out, one per source or research dimension, and mine primary
sources (papers, release notes, source code, benchmarks) into draft claims. Each
draft claim is recorded in [`prior-art/claims.yaml`](prior-art/claims.yaml) with
a kebab-case `id`, the `system` and pinned `version` it describes, the `claim`
prose, the measured `value`, a `source_url`, an `accessed_date`, and a
`confidence` with a `confidence_reason`. Claims are strictly **descriptive**:
they record what an upstream system does at a pinned version, never what
IronCache should do. Prescriptive IronCache decisions live in the design issues
and the ADRs, never in the claims file.

### 2. Independent adversarial verifier

A second, independent pass re-checks the load-bearing and lower-confidence
claims with a refute-by-default stance: the verifier tries to break each claim
against a fresh fetch of the primary source rather than confirm the miner's
reading. The verdict and evidence are recorded in each claim's `verification`
block (`confirmed` / `corrected` / `refuted` / `uncertain` / `self-verified`),
with a `best_source_url` and a `note` quoting the supporting text. Where the
verdict is `corrected`, `value` becomes the corrected value and the miner's
original reading is preserved under `original_value`. The same fan-out plus
adversarial-confirmation method was applied to the whole issue tree in the
pre-implementation audit; see [`AUDIT.md`](AUDIT.md) (re-verified claims carry
`verification.reaudited`).

The verifier and the miner are run as distinct passes so the check is genuinely
independent rather than the same agent grading its own homework.

### 3. Offline citation and uniqueness gate (live)

[`scripts/ci/check-prior-art-claims.sh`](../scripts/ci/check-prior-art-claims.sh)
is the hard, offline, deterministic gate and runs on every docs PR (workflow
[`docs.yml`](../.github/workflows/docs.yml)). It asserts:

- every claim `id` in `claims.yaml` is unique; and
- every bracketed `[id]` citation in the prose (PRIOR_ART, CHARTER, GLOSSARY,
  INVARIANTS, NON_GOALS, THREAT_MODEL, every `docs/design/*.md`, and every
  `docs/experiments/*.md`) resolves to a claim that exists in `claims.yaml`.

It does **not** re-fetch sources: upstream value drift is caught by
`accessed_date` going stale and by periodic re-verification, not by this script.
Its ADR sibling [`check-adr-index.sh`](../scripts/ci/check-adr-index.sh) applies
the same citation rule to ADR records. Together they guarantee the design tree
never cites a claim id that does not exist and never silently duplicates one.
This runbook is a process doc, not a design spec, so it is not in either
script's scan set; it still cites only ids that exist in `claims.yaml`.

### 4. Human merge gate (final authority)

A human PR review is the documented final authority over all agent output. There
is no agent auto-merge path: green CI is necessary but never sufficient. A
reviewer confirms the claim's source supports the stated value, that the
mechanism it backs respects the tenet order, and that any decision it settles is
recorded as an ADR per [adr/README.md](adr/README.md) and #4. A failed
verification quarantines the claim and blocks the mechanism that depends on it;
unsourced numbers are never merged.

## Harness-blocked: the trace-replay reproduction bar

The #94 design also specifies a stronger bar than citation hygiene: a mechanism
should enter the design tree only after its numbers are **reproduced** by
deterministic trace replay on N independent traces, banded
[lrb-model-and-traffic-reduction]. That bar was originally **deferred** as
harness-blocked. The engine and most of that harness are now built (the
benchmark and memory-model harness from #8; the conformance/differential/DST
stack from #95 and the Valkey differential oracle from #96), so the bar is no
longer blocked on a missing engine or oracle stack. The remaining piece is the
Belady oracle and the deterministic trace-replay reproduction that the strongest
form of this bar needs (#93).

Until that trace-replay reproduction lands, the live pipeline enforces the two
gates it *can* enforce offline: version-pinned sourcing plus the independent
adversarial re-check (steps 2 and 3 above). Numeric claims are admitted as
**cited and adversarially verified**, explicitly **not** as **reproduced**. When
it lands, the reproduction bar attaches as an additional, blocking gate on numeric
claims (a `verification.reproduced` verdict over the trace corpus), and this
runbook will be updated to make trace replay a merge requirement rather than a
deferred goal. The harness-blocked experiments are catalogued with the rest of
the deferred research design.

## Provenance summary

- `claims.yaml` is the single descriptive source of truth; prose agrees with it,
  and the file wins on any disagreement.
- Every load-bearing number in the prose carries an `[id]` into `claims.yaml`.
- Mining is adversarially verified; verification verdicts are recorded per
  claim; the offline gate enforces citation existence and id uniqueness in CI.
- Humans hold the merge gate; agents never auto-merge.
- Trace-replay numeric reproduction is specified (#94) but deferred until the
  harness (designed in #8, #95/#96; Belady oracle still open in #93) is built.

## References

- #94: the AI-assisted pipeline design this runbook realizes (decomposed from
  #88; the AI-Driven concern tracked into #127).
- #4: ADR index, decision register, and the citation/decision governance these
  gates plug into.
- #8, #95, #96, #93: the harness work that unblocks the trace-replay bar.
- [AUDIT.md](AUDIT.md): the pre-implementation application of this same
  fan-out-then-adversarial-confirm method to the whole issue tree.
