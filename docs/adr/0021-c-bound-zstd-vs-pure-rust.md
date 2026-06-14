# ADR-0021: C-bound zstd vs pure-Rust zstd for the static binary

Status: Accepted
Issue: #54

## Context

IronCache ships as one static binary (#81) that links zstd, the default value
codec already chosen in [ADR-0015](0015-default-value-codec.md) (#53). That ADR
fixed *which* codec sits in the default path (zstd at a low level); it explicitly
deferred *how* zstd is linked. This ADR resolves only that implementation
question and does not reopen the codec choice.

The question is whether the binary may carry a C dependency or must be pure Rust.
The mature `zstd` crate binds the zstd C reference library through `zstd-sys`
(zstd 0.13.3 binds zstd C 1.5.7 via zstd-sys 2.0.16+zstd.1.5.7)
[zstd-rust-crate-version], giving reference-exact output and the fastest path at
the cost of an `unsafe` FFI boundary and a C toolchain in the build. A pure-Rust
zstd backend would keep the binary provably memory-safe end to end, the purest
expression of the single-binary tenet, but on the evidence we have today it is
expected to trail the C reference on speed, ratio, and level/dictionary
configurability, and we have no pinned pure-Rust-zstd benchmark to size that gap
(the parity measurement is tracked as a research follow-up; see
[QUESTIONS.md](QUESTIONS.md)). The tradeoff resolves under our ranked tenets
(Compatible > Efficient > Simple > Scalable > AI-Driven).

## Decision

- **The C-bound `zstd` crate (`zstd` + `zstd-sys`) is the default codec
  implementation for the static binary**, with `zstd` and `zstd-sys` pinned to
  fixed versions so the build is reproducible and the linked libzstd is auditable
  [zstd-rust-crate-version]. zstd is statically linkable, so this still ships one
  self-contained binary; the concession is memory-safety purity, not the
  static-binary deliverable.
- **A pure-Rust zstd path is kept behind an off-by-default Cargo feature**,
  promotable to the default only once a pinned benchmark shows it matches the C
  library on ratio, throughput, and level/dictionary configurability for our
  payload profile. Until then it is not on any shipped path.

## Rejected Alternatives

- **Pure-Rust zstd as the default.** It is fully memory-safe and needs no C
  toolchain, which best serves the single-binary purity ideal in #81. Rejected
  for now: on present evidence it is expected to lag the C reference on speed,
  ratio, and configurability, and compression sits on the hot path for every
  cached value, so paying an unmeasured tax on every operation is not justified
  by a purity goal that Simple ranks below Efficient. The same pattern held for
  LZ4, where the pure-Rust lz4_flex trailed C lz4 on throughput at safe defaults
  [lz4-flex-safe-vs-c]; a pure-Rust zstd default would also risk subtle
  divergence from reference output and level semantics, threatening the
  Compatible tenet that zstd, as a wire and at-rest format, must honor. This
  option becomes the default only at measured parity, via the off-by-default
  feature above.

## Consequences

- The binary is **not provably memory-safe**: the `zstd-sys` FFI is an `unsafe` C
  boundary [zstd-rust-crate-version]. This is the key concession of this ADR. It
  is contained behind the `zstd` crate, and the linked libzstd is known exactly
  because `zstd` and `zstd-sys` are version-pinned, so the C surface is bounded
  and auditable.
- The build gains a C toolchain (cc/bindgen) for `zstd-sys`; the supply-chain and
  pinning discipline tracks the same artifact-auditing posture as the rest of the
  build.
- Default output is byte-for-byte zstd-reference compatible, so compressed values
  interoperate with every other zstd consumer, satisfying the Compatible tenet
  that ADR-0015 (#53) owes.
- The pure-Rust path stays a live fallback: if a future benchmark shows it
  underperforms, the C-bound path remains the supported default, and the Rust
  backend ships behind the off-by-default feature until it reaches parity. The
  choice is revisitable as pure-Rust zstd closes the gap, via a superseding ADR.
