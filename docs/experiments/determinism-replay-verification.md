# Research: Determinism replay-contract verification (Env-seam lint contract now, byte-identical cross-platform replay meta-test when the harness lands)

Issue: #160. Provisional decision: ADR-0003 (design the runtime for determinism) already pins the single-thread-per-shard runtime and the Env seam and states the replay contract; this doc splits #160 into the half that is actionable now (the Env-seam lint contract) and the half that is harness-blocked (the same-seed-twice byte-identical replay meta-test).

## Provisional decision (already pinned)

ADR-0003 (Accepted, issue #31) is the binding decision and this doc re-decides
none of it. The ADR pins three things: a single-thread-per-shard runtime; an Env
seam through which all clock, network, disk, and RNG access flows so that no code
on a decision path touches `std::time`, the network, the disk, or an RNG
directly; and the seed-and-replay contract, where a run is `(seed, ordered input
log)` and replaying the same pair must yield byte-identical eviction victims and
expiry firings because both consume only Env-supplied time and randomness. This
is the configuration that lets a whole simulation be driven and reproduced from
one seed [dst-fdb-tigerbeetle-single-seed]. ADR-0003 names two consequences this
doc operationalizes: that CI will lint against direct `std::time` / `Instant::now`
/ `rand` calls outside the approved Env abstraction once code lands, and that the
determinism replay CI gate is #160. RUNTIME.md (#25) owns the Env seam shape;
TESTING.md (#95) owns the DST path this contract protects.

The contribution of this doc is to split #160 along the line of what can ship
before the engine exists. The replay contract has two distinct enforcement
mechanisms with very different blocking status, and conflating them has kept the
whole issue parked behind the harness:

- The negative property (no module reaches a nondeterminism source outside the
  Env seam) is a static, source-level invariant. It does not need a running
  engine or a DST harness; it needs only code to scan. This is actionable as soon
  as the first module lands, and it is the cheaper, earlier guard.
- The positive property (replaying the same seed yields byte-identical execution,
  including across builds and platforms) is a behavioral invariant that requires
  the deterministic runtime, the Env simulator, and a captured input log. That is
  harness-blocked.

IronCache treats both as required, but ships the lint contract first as the
standing backstop and the meta-test second as the proof.

## The actionable-now half: the Env-seam lint contract

This half is an IronCache design choice and is not harness-blocked. It is a CI
contract that fails the build when any module reaches a nondeterminism source
outside the Env seam, enforcing the ADR-0003 negative property by construction.

Forbidden outside the Env seam (the denylist):

- Time: `std::time::Instant::now`, `std::time::SystemTime::now`, and any wall- or
  monotonic-clock read not routed through the Env clock.
- Network: `tokio::net`, `std::net`, and any direct socket construction not routed
  through the Env network.
- Disk: direct filesystem and `fsync` paths not routed through the Env disk.
- Randomness: `rand`, thread-local RNG, and any entropy source not seeded through
  the Env PRNG.

The contract is two enforcement layers, both required, so neither is a single
point of failure:

- A repository scan implemented as a CI script in the existing `scripts/ci/`
  house style (a `check-*.sh` sibling to `check-prior-art-claims.sh`), which
  greps the denylisted symbols across all crates and fails on any hit outside the
  one allowlisted Env-implementation module. The script is the cheap, fast,
  zero-dependency gate that runs on every PR. It is deliberately blunt: a textual
  denylist that errs toward false positives, since a false positive is a one-line
  allowlist annotation and a false negative is a silent determinism hole.
- A clippy/dylint lint as the semantic backstop that the grep cannot express:
  path-resolved detection that survives re-exports, aliasing, and macro expansion,
  so renaming an import does not smuggle a clock read past the textual scan. The
  lint resolves the fully-qualified path of each call and flags the same denylist,
  reported as a deny-level diagnostic in CI.

The allowlist is exactly one place: the Env-implementation module that binds the
production seam to the OS clock, sockets, disk, and entropy. Every denylisted
symbol is legal there and nowhere else. The allowlist is an explicit annotation
in source, not an implicit directory convention, so adding a new escape hatch is
a reviewable diff rather than a quiet exception.

Scope and known limits, stated honestly so the contract is not oversold:

- The lint catches direct reaches into the named std and crate paths. It does not
  catch nondeterminism laundered through a transitive dependency that itself reads
  the clock or the network internally; that class is caught only by the replay
  meta-test below. The two halves are complementary for this reason, not
  redundant.
- The lint is a source-level guard, so it runs with no engine and no harness. It
  is therefore the part of #160 that can land and start gating immediately, ahead
  of the runtime, which is the whole point of the split.

## The harness-blocked half: the byte-identical replay meta-test

This half is an IronCache design choice and IS harness-blocked. It is a meta-test
(a test of the test runtime, not of a command) that asserts the ADR-0003 positive
property: the same `(seed, ordered input log)` replays to byte-identical
execution.

Why this half is harness-blocked: proving byte-identical replay requires three
things that do not exist yet. It needs the deterministic single-thread-per-shard
runtime from ADR-0003 actually built; it needs the Env simulator (virtual clock,
in-memory network, seeded PRNG) bound to that runtime; and it needs a captured,
replayable input log format. Until those exist, the positive property can be
asserted on paper but not run, which is exactly why #160 has stayed blocked and
why the lint contract is split out to ship first. The single-seed-replay rationale
the meta-test rests on is the established DST property that all I/O and time are
simulated so any bug is reproducible from a single seed
[dst-fdb-tigerbeetle-single-seed].

## Experiment to run (the meta-test, once the harness lands)

Input and fixtures:

- A small library of seeds covering the decision surfaces that must replay
  identically: eviction-victim selection under memory pressure, expiry firing at
  TTL boundaries, RNG-driven tie-breaks, and any scheduling choice the runtime
  makes between ready shards.
- For each seed, an ordered input log: the exact sequence of client operations
  and Env events (clock advances, injected network and disk events) that the run
  consumed. The log plus the seed is the entire run description, with no shared
  state and no wall-clock dependence.
- A full-execution trace per run: the ordered eviction victims, the ordered
  expiry firings, and the reply bytes emitted, captured at a granularity fine
  enough that any divergence is a diff, not a summary mismatch.

The three replay legs, each a separate assertion:

- Same seed, same build, twice: run each seed twice in one process and once in a
  fresh process, and assert the two full-execution traces are byte-identical. This
  is the floor; failing it means the Env seam is leaking nondeterminism the lint
  did not catch (for example through a transitive dependency).
- Same seed, across builds: replay the captured `(seed, input log)` against a
  later build and assert the trace still matches the stored trace, so a refactor
  that silently perturbs ordering is caught.
- Same seed, across platforms: replay on the canonical target matrix (at minimum
  an x86_64 and an aarch64 target, Linux and the macOS dev target) and assert the
  traces are byte-identical across platforms, so endianness, float formatting, and
  hash-iteration order cannot diverge between hosts.

Fixed across all legs: the seed, the input log, the Env configuration, and the
trace granularity. Varied: the build (current vs later), the host platform, and
the process (same vs fresh). No timing numbers are measured here; this meta-test
is a byte-equality assertion, not a benchmark.

Logging discipline (the repro contract): the runner records the seed on every
failure AND on the periodic green runs, so a green log is a usable corpus of
replayable seeds and a red log is a one-line repro. A failing seed is the entire
bug report.

Decision rule:

- The replay gate passes only if all three legs are byte-identical for every seed
  in the library. Any divergence on any leg fails the gate and is filed with the
  diverging seed, since a single non-replayable seed invalidates every DST green
  and every "reproduces from one seed" claim that rests on it.
- The lint contract and the meta-test are both required and neither substitutes
  for the other: the lint is the cheap source-level backstop that runs without a
  harness, the meta-test is the behavioral proof that also catches laundered
  nondeterminism the lint cannot see.

## What would change the decision

- A determinism hole that the source-level lint cannot express (nondeterminism
  entering through a transitive dependency's internal clock or socket use) showing
  up only in the replay meta-test, which would justify extending the denylist to
  audited recorded-syscall checking rather than path-resolved linting alone.
- Cross-platform divergence traced to an unavoidable source (a dependency whose
  output is platform-dependent on a decision path), which would force either
  replacing that dependency or routing it through the Env seam, and would tighten
  the allowlist rather than widen it.
- The lint's false-positive rate proving high enough that the allowlist accretes
  exceptions, which would be the signal to move from a textual grep to the
  path-resolved dylint as the primary gate with the grep demoted to a fast
  pre-filter.
- The replay meta-test proving too slow to run the full cross-platform matrix per
  PR, which would split it into a per-PR same-build leg and a scheduled
  cross-build, cross-platform leg, keeping the byte-identical bar without flapping
  CI.

## References

- ADR-0003: design the runtime for determinism to enable DST (the binding Env
  seam, single-thread-per-shard, and seed-and-replay contract; issue #31). This
  doc operationalizes ADR-0003's stated consequence that CI lints against direct
  `std::time` / `Instant::now` / `rand` outside Env, and is the #160 replay gate it
  names.
- ADR-0002: shared-nothing thread-per-core (the model ADR-0003 builds on).
- Issues: #160 (this verification gate), #31 (the determinism decision), #95
  (the conformance/DST testing stack this protects), #100 (the seeded
  fault-injection catalog that consumes the same replay contract), #1 (vision
  EPIC).
- Specs: docs/design/RUNTIME.md (the Env seam shape, #25),
  docs/design/TESTING.md (the correctness stack and DST path, #95).
- Claims (resolved via docs/prior-art/claims.yaml): [dst-fdb-tigerbeetle-single-seed].
