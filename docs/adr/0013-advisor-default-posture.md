# ADR-0013: Advisor default posture is shadow/off

Status: Accepted
Issue: #155

## Context

The AI-Driven tenet is ranked last, and the project rule is that the cache must
be fully correct and fast with the advisor disabled. No issue yet committed the
advisor's default operating posture: off, shadow (observe and recommend), or
actively tuning on first run. This is the AI-Driven counterpart to the eviction
default-posture decision (ADR-0007), and pinning it protects the lower-ranked AI
tenet from silently overriding Compatible, Efficient, or Simple.

## Decision

The advisor ships **off by default, in shadow mode when enabled**: when turned
on it observes traffic and records recommendations (knob changes it would make)
but does not mutate live policy until an operator opts into active tuning.
Active autotuning is an explicit opt-in, gated by the safety guardrails (#91) and
the promotion gate (#154). The engine is fully correct and fast with the advisor
entirely disabled, and the advisor adds no runtime model-service, GPU, or network
dependency (NON_GOALS entry 11).

## Rejected Alternatives

- **Actively tuning on first run (on by default).** Rejected: it lets the
  lowest-ranked tenet change live behavior before any operator review, risking
  Compatible/Efficient/Simple regressions; learned-policy work belongs off the
  hot path and behind an opt-in [parrot-imitation-belady-icml20]
  [lecar-regret-minimization-smallcache].
- **No advisor surface at all by default (hard off, not even shadow-capable).**
  Rejected: shadow mode is the safe way to gather the evidence that justifies the
  advisor (#90 headroom study) without touching live policy; making it
  unavailable would block that evaluation. The default is off, but shadow is the
  first opt-in step before active tuning.

## Consequences

- Out of the box IronCache behaves identically with or without the advisor; the
  advisor cannot regress the engine because it is not in the decision loop until
  opted in.
- The opt-in ladder is: off (default) -> shadow (observe/recommend) -> active
  (tune within bounded knobs, with rollback and kill-switch, #91/#154).
- This complements the data-path non-goal (#13, no per-request inference) and the
  no-runtime-AI-dependency non-goal (#156): #13 says where ML may not run, #156
  says what it may not link, and this ADR says what it does by default.
