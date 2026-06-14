# Experiment: Single-core throughput bar vs Redis 8 / Valkey / Dragonfly / Garnet

Issue: #9. Provisional decision: ADR-0016 (headline metric is throughput-per-core, with p99/p999 alongside, under a fixed open-loop methodology).

## Provisional decision (already pinned)

The scoreboard is already fixed. ADR-0016 (`docs/adr/0016-headline-efficiency-metrics.md`,
Accepted, issue #7) pins **throughput-per-core** as the headline metric: total
throughput divided by cores used, so core count is not the lever. Tail latency
is reported alongside (p99/p999) from an **open-loop, coordinated-omission-corrected**
generator [coordinated-omission-closed-loop], not a closed-loop tool reporting
averages. Workloads use Zipfian keys and the YCSB operation mix
[ycsb-core-workloads] driven through pinned memtier with pipeline depth stated
explicitly [memtier-default-pipeline-1]. This experiment does NOT re-decide any
of that. It produces the numeric bar that ADR-0016 says the single-core study
(#9) must set, so #24 (concurrency model) and #26 (runtime) have a real target.

## Why this is harness-blocked

The per-core bar is an empirical number on real silicon. ADR-0016 fixes the rules
but supplies no values, and no value can be honestly written down without the
harness (#8) and the pinned competitor builds. The prior art cannot be borrowed
as numbers either: MSR published only log-scale graphs for Garnet with no absolute
ops/sec [garnet-bench-qualitative], Dragonfly's only single-core figure is parity
with an old Redis on m5.large [dragonfly-single-core-parity], and the headline
vendor gaps are vertical-scaling or TLS-on-TLS artifacts, not per-core wins
[dragonfly-25x-thread-asymmetry][keydb-tls-7x-claim]. So the bar must be measured
here, on one frozen single-instance pinned matrix, with symmetric thread counts,
before #24 can judge whether shared-nothing sharding earns its complexity.

## Experiment to run

Run topology (this is OUR setup, deliberately NOT a clone of any vendor matrix):
a single bare-metal instance with client and server taskset-pinned over loopback,
following the Valkey perf methodology so the NIC is never the bottleneck
[valkey-methodology-baremetal-pinning]. The exact canonical instance type, kernel,
and core layout are a frozen choice owned by the harness (#8) and recorded there;
this doc does not inherit a topology by implication. NOTE: we do NOT reproduce
Garnet's two-VM 72-vCPU accelerated-networking topology [garnet-bench-hardware];
that is a network-scaling setup and is the wrong shape for a per-core bar. We
borrow only its baseline set and workload shape, not its run topology.

Baselines: take Garnet's published baseline set [garnet-bench-baselines]
(Redis with tuned io-threads, KeyDB, Dragonfly, Garnet) and refresh to 2026
versions: Redis 8, Valkey, Dragonfly, Garnet, plus IronCache once it exists.
Key/value sizing follows the published comparison (8-byte keys and values) so the
workload shape is comparable to prior art even though the topology is not.

Pinning matrix (the canonical, frozen axis so results stay comparable over time):
server pinned to exactly 1 core; client given enough cores to never saturate;
record the exact instance type, kernel, and core layout (the values owned by #8).
Run the same matrix at 2 and 4 server cores only to confirm per-core scaling,
never as the headline.

Workload: memtier as the only load generator. Fixed params: 8-byte keys/values,
Zipfian key distribution (--key-zipf-exp 1), plaintext (no TLS), warmed to the
stated hit ratio before timing. Varied params: the YCSB operation mix
[ycsb-core-workloads] (A 50/50, B 95/5, C 100% read), and pipeline depth reported
as two separate, explicitly labeled lines: non-pipelined (pipeline=1, the memtier
default [memtier-default-pipeline-1]) and a stated pipelined depth.

Measured: absolute ops/sec, then ops/sec divided by server cores (the headline),
plus p50/p99/p99.9 from an open-loop fixed-rate run with coordinated-omission
correction [coordinated-omission-closed-loop] -- the absolute latencies MSR omitted
[garnet-bench-qualitative]. TLS is run as a separately labeled overhead line, never
folded into the plaintext bar [keydb-tls-7x-claim]. Thread counts are symmetric
across all systems so no result is a thread-asymmetry artifact
[dragonfly-25x-thread-asymmetry].

Decision rule: the recorded per-core ops/sec of the strongest plaintext competitor
at 1 core becomes the published bar. IronCache's "max throughput per core" thesis
is considered cleared only when its non-pipelined single-core ops/sec meets or
exceeds that bar AND its p99/p99.9 at a fixed offered rate are no worse, both under
this identical pinned matrix. A win that appears only at higher core counts is
recorded as scale-out, not as a per-core win, and does not clear the bar.

## What would change the decision

- A fresh single-core number from Dragonfly or Valkey against Redis 8 / Valkey 9.x
  that beats the current parity story [dragonfly-single-core-parity] would raise
  the bar IronCache must clear.
- Evidence that a measured per-core gap is the inline-on-receiving-thread network
  path rather than the engine would redirect #24's investment, not change the bar.
- A different canonical instance type or pinning layout, if adopted by #8, requires
  re-running the whole matrix; the bar is only comparable within one frozen matrix.

## References

- Issues: #9 (this), #7 (parent), #24 (concurrency model), #26 (runtime), #1, #8 (harness)
- ADR-0016 `docs/adr/0016-headline-efficiency-metrics.md` (pins the metric)
- Research: `docs/research/garnet.md`, `docs/research/dragonfly.md`,
  `docs/research/redis-core.md`, `docs/research/valkey.md`, `docs/research/keydb.md`,
  `docs/research/benchmarking-correctness.md`
- Claims: [garnet-bench-hardware], [garnet-bench-baselines], [garnet-bench-qualitative],
  [dragonfly-single-core-parity], [dragonfly-25x-thread-asymmetry], [keydb-tls-7x-claim],
  [valkey-methodology-baremetal-pinning], [coordinated-omission-closed-loop],
  [memtier-default-pipeline-1], [ycsb-core-workloads]