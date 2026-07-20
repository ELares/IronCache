<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache cluster benchmark: 3-node horizontal scaling on AWS Graviton (2026-06-17)

The first multi-node head-to-head IronCache against itself: does a 3-node cluster do
roughly 3x the throughput of a single node? This validates the Wave 3 clustering work
(slices 1-2: CRC16/XMODEM slots, the static slot map, MOVED redirects, the multi-node
CLUSTER SLOTS/SHARDS/NODES projection) on real hardware with a real Redis-cluster client.

## Setup

- **Nodes:** 3x `t4g.micro` (AWS Graviton, aarch64, 2 vCPU burstable, ~1 GiB) co-located
  in one VPC/subnet/AZ. Each runs the released `linux-arm64-musl` ironcache binary
  (2026.0617.2), `shards = 2`, `maxmemory = 600mb`, `maxmemory-policy = allkeys-lru`,
  `cluster_enabled = true`, a per-node `cluster-announce-id`, and the SAME static
  `[[cluster_topology.nodes]]` block splitting the 16384 slots `[0,5461] / [5462,10922] /
  [10923,16383]` across the three private IPs. No `redis-cli --cluster create` needed (slice
  3); the static config IS the cluster.
- **Load generator:** a separate instance running `memtier_benchmark --cluster-mode`, which
  reads `CLUSTER SLOTS`, computes each key's slot, and routes it directly to the owning node
  (following MOVED on a miss) - exactly like go-redis / lettuce / `redis-cli -c`.
- **Workload:** 90% GET / 10% SET, 128-byte values, 300k-key space, prefilled so GETs hit.

## Result: ~2.8x throughput for 3 nodes (near-linear)

| metric | single node (standalone) | 3-node cluster | ratio |
| --- | ---: | ---: | ---: |
| throughput (ops/sec) | 150,459 | **422,304** | **2.81x** |
| p50 latency | 1.375 ms | 0.575 ms | |
| p99 latency | 4.767 ms | 3.119 ms | |
| MOVED/sec (steady state) | n/a | **0** | |

Both measured with the SAME load generator (8 vCPU, ~33% idle during the 3-node run and
~80% idle during the single-node run, so the generator was NOT the bottleneck - the cap is
the nodes). An earlier pass on a smaller 4 vCPU generator was generator-bound at ~317k
(3-node) / 163k (single), which is why the generator was upsized for the clean number above.

**Reading it:** 3 nodes do 2.81x a single node = ~94% scaling efficiency. The gap from a
perfect 3.0x is (a) `t4g.micro` is burstable - sustained throughput depends on CPU credits,
which vary run to run, and (b) cluster mode adds a per-keyed-command slot-ownership check
(the MOVED gate) that a standalone node skips. The **0 MOVED/sec** in steady state confirms
the client learned the slot map and routed every key to its owner directly - the cluster
contract (CLUSTER SLOTS + MOVED) works end to end with an unmodified cluster client.

## Correctness verified on the live cluster

- `CLUSTER SLOTS` returned all three ranges with the right private IPs and node ids.
- `CLUSTER INFO` -> `cluster_enabled:1`, `cluster_state:ok`, `cluster_slots_assigned:16384`.
- Raw (non-cluster) `SET` against node 1 returned the correct redirects: `foo` (slot 12182)
  -> `MOVED 12182 <node3>`, `qux` (slot 9995) / `zap` (slot 6469) -> `<node2>`, and
  `bar` / `baz` (slots in 0-5461) -> served locally.

## Honesty notes

- INDICATIVE, not publishable: `t4g.micro` burstable nodes + a shared-tenant VM; the
  authoritative number wants non-burstable, dedicated instances. The scaling SHAPE
  (near-linear to 3 nodes) is the takeaway, not the absolute ops/sec.
- The cluster was formed by static TOML config (slice 2). Self-formation via
  `redis-cli --cluster create` (the CLUSTER MEET/ADDSLOTS/SETSLOT mutators) is slice 3;
  online resharding (ASK) is slice 4; replication + failover are the later HA tier. None of
  those affect steady-state sharded throughput, which is what this benchmark measures.
- All AWS resources were torn down after the run.

## Reproduce

Cross-build (or download the `linux-arm64-musl` release), configure 3 nodes with a shared
`cluster_topology` + per-node `cluster-announce-id` (see the slice-2 integration test
`crates/ironcache/tests/cluster_slice2.rs` for the exact shape), boot each with
`ironcache --config nodeN.toml server`, then drive with
`memtier_benchmark --cluster-mode -s <node1-ip> -p <port> --ratio 1:9 --data-size 128`.
