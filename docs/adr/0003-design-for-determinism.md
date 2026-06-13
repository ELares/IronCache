# ADR-0003: Design the runtime for determinism to enable DST

Status: Accepted
Issue: #31

## Context

Deterministic Simulation Testing (DST) is a first-class verification strategy
for IronCache, not a retrofit. DST only works if every source of nondeterminism
(time, network, disk, scheduling, RNG) is funneled through a controllable seam
and the concurrency model produces a reproducible execution order. Both are
structural choices that are cheap now and ruinous to bolt on later. This builds
on the shared-nothing model (ADR-0002).

## Decision

Build on a single-thread-per-shard runtime in which all environment access
(clock, network, disk, RNG) goes through a controllable `Env` seam, so a
recorded input log replays to byte-identical eviction and expiry decisions
[dst-fdb-tigerbeetle-single-seed]. No code on a decision path calls the clock,
the network, the disk, or an RNG directly; it goes through `Env`.

## Rejected Alternatives

- **Ambient access to `std::time`, sockets, and `rand` from anywhere.**
  Rejected: nondeterminism leaks in everywhere and DST becomes impossible; a
  failing run cannot be replayed from a seed.
- **Multiple threads per shard.** Rejected: cross-thread interleavings within a
  shard are nondeterministic and re-introduce the shared-state hazards ADR-0002
  removed.

## Consequences

- DST is buildable by construction and gates the testing stack (#95), the
  Jepsen/Elle plan (#99), and the determinism replay CI gate (#160).
- This is invariant 2; CI will lint against direct `std::time` / `Instant::now`
  / `rand` calls outside the approved `Env` abstraction once code lands.
- The `Env` seam is the single integration point for the simulated clock, the
  fault injector (#100), and the deterministic scheduler.
