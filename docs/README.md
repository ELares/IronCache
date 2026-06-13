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

The authoritative, evolving design lives in the
[GitHub issues](https://github.com/ELares/IronCache/issues), indexed from the
[vision EPIC (#1)](https://github.com/ELares/IronCache/issues/1) and grouped by
milestone (M0 Vision and Scope, M1 Architecture Specification, M2
Prototype-Ready Design).

Prose in this project uses no em dashes or en dashes.
