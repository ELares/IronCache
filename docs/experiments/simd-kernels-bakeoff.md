# Experiment: SIMD register-histogram / max-merge kernels vs scalar (benchmark-gated)

Issue: #116. Provisional decision: scalar-first, SIMD off by default behind a
feature gate; ADR-0016 supplies the throughput-per-core decision rule.

## Provisional decision (already pinned)

The correctness-first posture is settled by #39 and is not re-decided here:
IronCache lands scalar PFCOUNT/PFMERGE/BITCOUNT first for a correct, Compatible
implementation, then evaluates SIMD as an isolated optimization. SIMD doing the
work up front is rejected as an Efficiency play that would complicate the first
correct path. So the provisional decision is scalar is the default and SIMD ships
off by default behind a Cargo feature gate, enabled only where it measurably
wins. The headline metric and methodology are ADR-0016 (throughput-per-core,
with p99/p999 alongside under a fixed open-loop generator); this doc does not
re-decide the metric, it produces the per-kernel numbers that justify flipping
the gate. Note the motivating prior art is narrower than a vectorization win:
Redis 8.0's reported up to 112 percent throughput gain is attributed primarily to
its reworked io-threads model (socket I/O threading), not per-command
vectorization [valkey-redis8-iothreads-112], so the SIMD case must be earned by
measurement, not borrowed.

## Why this is harness-blocked

The decision rule is a measured throughput-per-core delta on real silicon, which
needs three things that do not exist yet: the benchmark harness and ADR-0016
methodology (#8); a working scalar PFCOUNT/PFMERGE/BITCOUNT path (#115 dense
registers and the string/bitmap path) to be the baseline; and a SIMD kernel
implementation behind the feature gate to compare against. Until the scalar path
and the harness exist, any scalar-vs-SIMD ranking is a citation comparison across
mismatched hardware, not a result.

## Experiment to run

Kernels under test (the three hot inner loops that are data-parallel over a
contiguous byte block):

- Register histogram for PFCOUNT: count, per register value 0..63, how many of
  the 16384 6-bit dense registers hold it. Scalar unpacks 6-bit lanes and bins;
  the SIMD variant unpacks and bins in vector lanes.
- Register max-merge for PFMERGE: per-register max across N dense register blocks.
  Scalar is a byte-wise max loop; the SIMD variant is a lane-wise max over
  unpacked registers.
- BITCOUNT popcount over a bitmap value: scalar word-at-a-time popcount vs a SIMD
  popcount kernel.

Fixed parameters (held identical across scalar and SIMD):

- ADR-0016 methodology: throughput-per-core headline, open-loop and
  coordinated-omission-corrected tail, server pinned, on the #8 frozen instance.
- Identical inputs per kernel: the same dense register block (full and sparse-
  promoted-to-dense), the same multi-source PFMERGE fan-in, the same bitmap sizes.
- Same build except the SIMD feature gate, so the only variable is the kernel.

Varied parameters:

- Kernel implementation: scalar baseline vs SIMD (portable-SIMD and/or a
  target-feature path), one row per kernel.
- Input size sweep: small (sparse-promoted), one full dense HLL, and a wide
  PFMERGE fan-in; bitmaps from small to large for BITCOUNT.
- Register/CPU width where the SIMD path is gated on a detected target feature.

Measured:

- Throughput-per-core per kernel, scalar vs SIMD, as the headline delta.
- p99/p999 of the command containing the kernel (PFCOUNT, PFMERGE, BITCOUNT)
  under the open-loop generator, so a throughput win that costs tail latency is
  visible.
- A byte-exact equality check: the SIMD kernel output must equal the scalar
  output bit-for-bit on every input (the histogram bins, the merged registers,
  the popcount), so SIMD never changes a result.

Decision rule:

- Flip the feature gate on for a kernel only if SIMD shows a throughput-per-core
  win (ADR-0016) AND introduces no correctness difference AND no p99/p999 tail
  regression on its command. A kernel that does not clear all three stays scalar.
  The gate is per-kernel, so PFCOUNT can be vectorized while PFMERGE is not.

## What would change the decision

- A SIMD kernel that wins throughput-per-core with zero correctness delta and no
  tail regression flips that one gate on by default for the detected target.
- A SIMD kernel that wins throughput but regresses p99/p999 stays gated off,
  available only as an explicit opt-in build, never the default.
- Evidence that the measured command-level gain is the io-threads/network path
  rather than the kernel [valkey-redis8-iothreads-112] redirects the work away
  from vectorization and leaves all kernels scalar.

## References

- Issues: #116 (this), #39 (parent), #115 (scalar HLL path), #8 (harness),
  #97/#98 (correctness oracle and model tests).
- ADR-0016 `docs/adr/0016-headline-efficiency-metrics.md` (pins the metric and
  the open-loop methodology that the decision rule uses).
- Claims: [valkey-redis8-iothreads-112].
