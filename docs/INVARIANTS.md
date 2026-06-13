# Load-bearing invariants

The properties every design, ADR, and review must respect. They are stated once
here, ranked-tenet aware, and made CI-checkable where the language allows.
Companion to [GLOSSARY.md](GLOSSARY.md); both roll up to issue #3. Conflicts
resolve in tenet order: Compatible > Efficient > Simple > Scalable > AI-Driven.

1. **Shared-nothing.** A key's shard is owned by exactly one core; there is no
   hot-path shared mutable state, and cross-shard work is explicit message
   passing [seastar-shared-nothing]. This replaces Redis's single command thread
   [redis-command-execution-single-threaded] with per-core ownership. Owner: #24.
2. **Determinism.** Time, network, disk, and RNG are reached only through
   abstractions, so a seeded replay of the same input yields byte-identical
   eviction and expiry decisions. No direct clock or rand calls on decision
   paths. Owner: #31.
3. **Memory honesty.** `maxmemory` is accounted against allocator-attributed
   bytes, not naive object sizes [redis-maxmemory-accounting], so the limit is an
   honest bound rather than a fragmentation-blind estimate. Owner: #41.
4. **No fork.** No persistence, snapshot, or maintenance path may call `fork()`.
   Copy-on-write RSS doubling is avoided structurally; reclamation uses
   background threads, not process forks. Owner: #59.
5. **Behavioral-equivalence contract.** The compatibility bar is behavioral
   equivalence with the Valkey/Redis oracle, not bit-identical replies. Reply
   structure, types, and observable side effects must match; the precise
   error-text bar is set by the conformance issue. Behavioral equivalence is
   chosen over bit-identity because it preserves client compatibility (the top
   tenet) without freezing the internal representations the Efficient tenet
   needs to evolve. Owner: #95.

## CI-checkability

Where a property is mechanical, CI will enforce it once the engine code lands:

- Invariant 4: a lint forbids `fork()` and any libc fork binding anywhere in the
  tree.
- Invariant 2: a lint forbids direct `std::time`, `Instant::now`, and `rand`
  calls outside the approved abstraction modules.
- Invariant 1: the hot-path crate denies `std::sync` lock and shared-atomic
  imports.

Invariants 3 and 5 are enforced by test suites owned by their issues (#41, #95),
not by lints. These lints are specified here and become active with the first
Rust crate; until then this file is the contract reviewers cite.

## Ownership map

| Concern | Issue |
| --- | --- |
| Shared-nothing / shard ownership | #24 |
| Cross-shard coordinator and message passing | #29 |
| Determinism and seeded-replay testing | #31 |
| Memory-honesty accounting and allocator | #41 |
| No-fork reclamation and background drop | #59, #51 |
| Behavioral-equivalence and error-text bar | #95 |
| Encoding terminology and `OBJECT ENCODING` | #40 |
| Tenets and conflict ordering | #2 |
| ADR cross-reference | #4 |
| Coherence enforcement across issues | #5 |
