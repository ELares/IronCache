# ADR-0016: Headline metrics are throughput-per-core and memory-at-fixed-hit-ratio

Status: Accepted
Issue: #7

## Context

IronCache's thesis is "the most efficient cache in the world." Efficient must be
a number, not a vibe, and the wrong number (aggregate QPS on a high-core box) is
the exact metric every vendor benchmark war has gamed. This fixes the scoreboard
and the methodology so every comparison runs the same way.

## Decision

The headline metrics are **throughput-per-core** and **memory-at-a-fixed-hit-
ratio**, with **p99/p999 tail latency** reported alongside, under a fixed
methodology:

- Per-core throughput: total throughput divided by cores used, so core count is
  not the lever.
- Memory: resident bytes-per-stored-item at a stated, fixed hit ratio.
- Tail latency measured with an open-loop, coordinated-omission-corrected
  generator [coordinated-omission-closed-loop], not a closed-loop tool reporting
  only averages.
- Workloads: Zipfian key distributions and the standard YCSB operation mix
  [ycsb-core-workloads], driven through the pinned memtier harness (not YCSB's
  JVM client, which bottlenecks throughput and lacks pipelining), with pipelining
  depth stated explicitly (memtier defaults to none [memtier-default-pipeline-1]).
- Comparisons run against a pinned Valkey/Redis/Dragonfly on identical hardware.

## Rejected Alternatives

- **Aggregate QPS on a high-core box (the vendor default).** Rejected: it rewards
  core count, not efficiency. It is the lever behind the marketing "25x" that
  pits 64 threads against 2 [dragonfly-25x-thread-asymmetry], while at one core
  the shared-nothing leader is only at parity with Redis
  [dragonfly-single-core-parity]. Per-core is the honest bar.
- **Throughput only, no memory metric.** Rejected: a cache that is fast but
  fat loses on total cost; memory-per-item at a fixed hit ratio is half the
  efficiency story and is where the encodings, allocator, and compression
  decisions are judged.
- **Closed-loop latency with averages.** Rejected: it hides tail latency via
  coordinated omission [coordinated-omission-closed-loop]; p99/p999 under an
  open-loop generator is the honest measure.

## Consequences

- The benchmark harness (#8) and the regression gate (#159) implement exactly
  this methodology; the single-core bar study (#9) sets the numeric targets.
- This is the Efficient half of the per-tenet acceptance gates (#157, ADR-0017).
- No headline efficiency number ships without a reproducible run under this
  methodology (NON_GOALS entry 9).
