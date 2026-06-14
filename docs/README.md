# IronCache docs

This is the design record for IronCache while it is in its research and
specification phase. No engine code exists yet; these documents and the GitHub
issues are the specification.

- [PRIOR_ART.md](PRIOR_ART.md): the version-pinned comparative survey of Redis,
  Valkey, KeyDB, DragonflyDB, Memcached, Garnet, and the academic caching
  literature, with what IronCache borrows, adapts, or rejects from each.
- [prior-art/claims.yaml](prior-art/claims.yaml): the single source of truth for
  every numeric or version-specific prior-art claim, with sources, pinned
  versions, confidence levels, and an independent verification verdict. Checked
  in CI by [`../scripts/ci/check-prior-art-claims.sh`](../scripts/ci/check-prior-art-claims.sh).
- [research/](research/): the per-dimension research corpus (one document per
  caching dimension) and the machine-readable [`corpus.json`](research/corpus.json).
- [CHARTER.md](CHARTER.md): the thesis, the five ranked tenets and their
  conflict order, and the governing-document index.
- [GLOSSARY.md](GLOSSARY.md) and [INVARIANTS.md](INVARIANTS.md): the canonical
  vocabulary and the load-bearing invariants every design must respect.
- [design/](design/): the subsystem design specifications that gate implementation.
- [experiments/](experiments/): experiment-design records for harness-blocked
  research (the provisional decision plus the exact benchmark to run once the
  engine and harness exist).
- [THREAT_MODEL.md](THREAT_MODEL.md): the shared adversary model (assets, trust
  boundaries, attacker capabilities, STRIDE per subsystem, in-scope vs accepted
  risk) that the security specs hang off, paired with the root
  [SECURITY.md](../SECURITY.md) policy.
- [adr/](adr/): the Architecture Decision Records, their registers
  ([INDEX](adr/INDEX.md), [OPEN](adr/OPEN.md), [QUESTIONS](adr/QUESTIONS.md)),
  and the ADR format.
- [ROADMAP.md](ROADMAP.md): the implementation-readiness sequencing (thin
  vertical slice, waves, gate set). [AUDIT.md](AUDIT.md): the pre-implementation
  audit record.

The authoritative, evolving design lives in the
[GitHub issues](https://github.com/ELares/IronCache/issues), indexed from the
[vision EPIC (#1)](https://github.com/ELares/IronCache/issues/1) and grouped by
milestone (M0 Vision and Scope, M1 Architecture Specification, M2
Prototype-Ready Design).

Prose in this project uses no em dashes or en dashes.
