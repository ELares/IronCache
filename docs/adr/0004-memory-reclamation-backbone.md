# ADR-0004: Memory-reclamation backbone

Status: Accepted
Issue: #33

## Context

A lock-free reclamation choice would normally be made before the index, the
hybrid log, and the snapshot path are built. But under shared-nothing
thread-per-core (ADR-0002), a shard's store is owned by exactly one core and no
other core may hold a reference to it, so the question is narrower than it first
appears: where, if anywhere, does the engine genuinely share a structure across
cores, and what reclaims it safely?

This ADR overrides issue #33's stated recommendation (adopt a custom FASTER-style
global-epoch + drain-list as the backbone, with crossbeam-epoch studied but not
the integration point). That recommendation assumed an in-place writer racing a
concurrent reader; under single-owner shared-nothing (ADR-0002) there is no
concurrent reader on the owned hot path, so the premise does not hold and the
lighter decision below is the correct one.

## Decision

- The shard-local hot path uses **no safe-memory-reclamation (SMR) machinery**.
  Single-owner access (ADR-0002) makes deferred free unnecessary; frees are plain
  drops by the owning core.
- For the rare structure that is genuinely shared across cores off the hot path
  (for example a cross-shard frequency sketch for global hot-key detection), use
  off-the-shelf **`crossbeam-epoch`** rather than a bespoke framework.
- Defer adopting a custom FASTER-style global-epoch + thread-local + trigger
  drain-list framework until the HybridLog region-shift work (#64) demonstrates
  that epoch-based reclamation is insufficient for phase-aware region boundary
  moves. The epoch-vs-custom-framework contest that #33 posed for any future
  shared or region-shift case is NOT resolved here; it is decided at #64. Only
  the hot-path question (no SMR) is resolved now.

## Rejected Alternatives

- **Adopt a custom FASTER-style drain-list framework now.** Rejected as
  premature: it is phase-aware, epoch-protected machinery [faster-epoch-protection]
  with real maintenance cost, and nothing on the owned hot path needs it yet;
  building it before #64 proves the need violates Simple.
- **Make a hyaline/`seize`-style reclaimer (papaya) the default backbone.**
  Rejected: hyaline is more robust than EBR for heavily-shared lock-free maps
  [seize-vs-epoch] [papaya-version-reclamation], but the per-shard store is not
  shared, so a global reclaimer on the hot path is unnecessary overhead.

## Consequences

- The per-shard primary store is a plain unsynchronized map (ADR-0005); its
  reclamation is trivial.
- Any future shared lock-free structure has a named, low-risk default
  (`crossbeam-epoch`).
- The decision is explicitly revisited at #64 if region-shift draining needs
  phase awareness; a reversal would be a new ADR superseding this one.
