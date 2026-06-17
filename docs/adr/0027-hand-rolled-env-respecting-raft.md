# ADR-0027: Hand-rolled, Env-respecting Raft, verified in deterministic simulation

Status: Accepted
Issue: #73

## Context

The control plane (CONTROL_PLANE.md, #73) owns the authoritative cluster config
via Raft: the slot-to-node map (ADR-0025), the monotonic config epoch, the
membership roster, and replica role assignments. The clustering experiment notes
for #73 lean toward "use a verified Raft library rather than hand-roll
consensus." That preference is reasonable in the abstract, but it conflicts with
three constraints this codebase already ships and enforces:

1. Determinism (ADR-0003). Every read of the clock and every random draw must go
   through the `ironcache-env` seam (`Clock` / `Rng`), and protocol logic must
   be a pure step function so a run replays byte-identically from a seed. The
   HA-2 simulation harness (`ironcache-sim`) is built entirely on this seam.
2. The invariant lint (scripts/ci/check-rust-invariants.sh) forbids foreign
   `std::time`, foreign `rand`, and foreign async executors in the protocol
   crates, and forbids `std::sync` locks on the hot path.
3. The DST replay obligation (JEPSEN_PLAN.md, the #68 single-seed convention): a
   failing run must reproduce from one seed, so the consensus engine has to be
   drivable in virtual time, single-stepped by the harness.

No mature off-the-shelf Raft crate satisfies all three. raft-rs, openraft, and
async-raft each own their clock (they read `Instant::now` / `tokio::time`), own
an RNG for election jitter, and assume a real async executor. None is generic
over an injected clock-plus-RNG seam, and none can be single-stepped in virtual
time for byte-identical seeded replay. Wrapping one would either leak foreign
time / rand / executor (a lint violation that breaks determinism) or require a
fork so deep that we own the correctness surface anyway, with less visibility
than code we wrote.

The experiment note's underlying intent is correctness assurance: do not trust
an unproven consensus implementation. That intent is legitimate and is preserved
below; only the literal "import a library" mandate is rejected.

## Decision

Implement a hand-rolled Raft control plane as a pure step machine in a new leaf
crate `ironcache-raft`. The engine is generic over a small storage-plus-RNG
seam and emits an `Effects` set (sends, timer operations, applies, persistence);
it performs no I/O and depends on no transport. All time and randomness flow
through the Env seam: in tests via the HA-2 `SimNode` adapter (`SimCtx::now` /
`gen_below` / `set_timer`), in production (HA-4) via the same engine wired to a
clusterbus adapter and `ironcache-env`'s real `Clock` / `Rng`. The same compiled
engine is what tests exercise and what production ships; only the adapter differs.

The replicated state machine is config only: the slot map, the monotonic epoch,
the membership roster, and replica roles. User data never enters the log.

We replace "trust a verified library" with "prove it in deterministic simulation
against the documented failure bar," a stronger, repo-native guarantee than
importing a crate verified against its own model:

- The HA-2 DST harness is the proof substrate: every sub-slice is a `SimNode`
  exercised under partition / latency / drop fault injection with seeded replay.
- The Jepsen / Elle gate (JEPSEN_PLAN.md, #99) is the acceptance bar: each Raft
  safety property maps to specific DST scenarios and specific Redis-Raft failure
  classes, and a sub-slice does not merge until its mapped scenarios are green
  over a seed sweep.
- Each sub-slice gets a paper-anchored adversarial review (Ongaro/Ousterhout
  sections 3 to 6), the same review discipline that has caught real bugs every
  PR on this codebase.
- Safety invariants (Election Safety, Log Matching, Leader Completeness,
  State-Machine Safety, committed-entry durability, and no-two-owners-per-epoch)
  run as harness assertions after every scenario, not as one-off spot checks.

## Rejected Alternatives

Wrap a mature Raft crate (raft-rs, openraft, async-raft) behind an adapter.
Rejected: each owns its clock (`Instant::now` / `tokio::time`), its RNG for
election jitter, and an async executor. Interposing the `ironcache-env` seam is
not supported by their APIs, so the engine could not be single-stepped in
virtual time and a run could not replay byte-identically from a seed. This
breaks ADR-0003 and fails the invariant lint and the DST replay obligation.

Fork a crate and rip out its time / rand / executor. Rejected: the fork would be
deep enough that we own the correctness surface anyway, with less visibility
than code we wrote, plus a permanent rebase burden against upstream.

Defer real consensus: ship a single-voter degenerate control plane first and add
multi-voter Raft last. Rejected: the degenerate path exercises none of the
election / replication safety logic, so it would mask exactly the bugs that
matter and give false confidence; it also could not be tested against the
partition / membership failure classes the bar requires.

Skip the control plane and keep the slot map authoritative per node (gossip /
point-to-point sync). Rejected by ADR-0025 / CONTROL_PLANE.md already: an
unsynchronized per-node map cannot give linearizable slot ownership (no
two-owners-at-one-epoch), which is the property the cluster contract depends on.

## Consequences

Positive: full Env-determinism is preserved; there is zero foreign
time / rand / executor; the consensus surface is auditable Rust we own; the same
engine ships to production transport unchanged; correctness is proven against our
fault model and the documented failure bar, with seeded replay of any failure.

Negative and residual risk: we own all of Raft's correctness, including its
subtlest corners (commit-only-current-term, conflict truncation, single-server
membership safety, snapshot / log-index interaction). A hand-rolled engine can
harbor a safety bug a battle-tested crate would not. The mitigations are the
sub-slicing (each PR's correctness surface is small enough to cover fully),
the DST seed sweeps over the exact nemeses that break hand-rolled Raft, the
paper-anchored review, and the #99 suite running later against the real
multi-node cluster (HA-4 onward) as the model-versus-reality backstop. We
explicitly accept that DST proves correctness against the modeled fault catalog;
faults outside it are out of scope, as they would be for any Raft crate.

This refines ADR-0003 (determinism) and sits under ADR-0025 (the partition map
the log replicates) and ADR-0026 (the replication / consistency model). It is
gated by #99 (the Jepsen acceptance bar).
