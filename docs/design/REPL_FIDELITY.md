<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Design: per-replica replication fidelity (real endpoints, N replicas, true lag)

Issue: #365 (the per-replica fidelity remainder; the structured `/topology` read
core landed in #439). Decisions: ADR-0002 (shared-nothing thread-per-core),
ADR-0003 (determinism / Env seam), ADR-0026 (the min-replicas-max-lag in-sync
gate). Related: REPLICATION.md (the link / full-sync / tail design),
REPLICA_READ.md (the in-sync read gate), CLUSTER_CONTRACT.md (the slot map + node
table), #439 (the structured `/topology` read, `crates/ironcache/src/topology.rs`).

## Goal and scope

`INFO # Replication` and `CLUSTER SHARDS` must report each connected replica with
its REAL advertised endpoint, its REAL acked offset and lag, and they must model
MORE THAN ONE replica per primary. The structured `/topology` endpoint (#439) must
carry the same per-replica detail. Today none of this is true (#365).

This spec is OBSERVABILITY-only. It MUST NOT alter the full-sync / resume decision,
the offset tracking that feeds the in-sync gate (ADR-0026), or the `InSyncReplicas`
quorum count. It only changes what the primary RECORDS and REPORTS about its
connected replicas.

## Current state (what the code does today)

Fact-checked against the source on 2026-06-30:

1. **Replica endpoints are placeholders.** `dispatch.rs::replication_info` emits the
   `slaveN:` line with `ip = ""`, `port = 0`; the inline comment says "The master
   does not learn the replica's advertised client endpoint over the current
   handshake (HA-7d sends node id + ack only), so report a placeholder endpoint."

2. **The replica advertises nothing usable.** The production replica handshake sends
   `Frame::ReplConf { node: 0, ack, resume_token }` (`replica_attach.rs:1270`); the
   `node` field is documented "advisory to the primary's link bookkeeping." So the
   primary never learns the replica's identity OR endpoint. The other handshake
   call sites that pass a non-zero `node` are tests.

3. **Single-replica model.** `ReplNodeStatus` (`lag.rs`) tracks ONE replica:
   `connected_slaves` (0 or 1) plus a single `slave_offset` atomic. There is no
   per-replica table, so a primary with two replicas reports one.

4. **Peer offset / health are the queried node's own.** `CLUSTER SHARDS` renders the
   queried node's own link from the real status cell, but other nodes' replica rows
   are hardcoded (`role master, replication-offset 0, health online`).

## The id-space (the canonical mapping, fact-checked against the source)

There are TWO id forms in the tree; getting them straight is essential, because a
naive `slot_id` round-trip is WRONG for production ids.

1. **The cluster announce id** is a 40-lowercase-hex STRING. In production it is a
   RANDOM 160-bit id, `serve.rs::node_id_hex` = `format!("{a:016x}{b:016x}{c:08x}")`
   over three RNG draws. It is NOT derived from a `u64`.

2. **The transport `NodeId(u64)`** is DERIVED FROM the announce id by
   `ironcache_raft_net::node_id_from_announce`, which takes the FIRST 16 hex chars of
   the announce id as the `u64`:

   ```rust
   // raft-net/src/lib.rs
   pub fn node_id_from_announce(id: &str) -> NodeId {
       if id.len() >= 16 {
           if let Ok(v) = u64::from_str_radix(&id[..16], 16) { return NodeId(v); }
       }
       /* deterministic FNV-1a fallback for a non-hex id (unreachable for a valid id) */
   }
   ```

   This is the SINGLE source of truth, reused by the leader-hint resolution and the
   slot-map; the system keeps the first-16-hex prefixes unique across members.

`slot_id(NodeId) = format!("{:040x}", id.0)` (`raft/src/lib.rs`) is a SEPARATE,
raft-INTERNAL synthesis used only to mint a cluster id FROM a `NodeId` in the
sim / internal path (leading-24-zeros form). It is NOT the production announce id and
is NOT the inverse of `node_id_from_announce`; do not use it to resolve a production
replica.

**Resolution (the correct mechanism).** Given the replica's `NodeId(v)` (from its
`REPLCONF`), the primary finds the `SlotMap` member `m` whose
`node_id_from_announce(m.id) == NodeId(v)`, then uses `m`'s `(host, port)`. This is
exactly the reverse lookup the leader-hint resolution already performs, O(members) on
the rare `INFO` read. The blocker is purely that the replica advertises `0` today.

## Design

### 1. The replica advertises its real identity (the unblocking change)

Populate `Frame::ReplConf.node` with `node_id_from_announce(self_announce_id).0` (the
replica's own announce id is `ctx.info.cluster_node_id`, already threaded into
`replica_attach.rs` as `self_node_id`) instead of `0`. The frame field ALREADY EXISTS
and round-trips (`frames.rs`), so this is wire-backward-compatible: an older primary
ignores it (it is advisory today), a newer primary resolves it. Use
`node_id_from_announce` (NOT `u64::from_str_radix` over the whole 40-hex string, which
overflows a `u64` for a random 160-bit id) so the value is the SAME `NodeId` the rest
of the system derives. In STANDALONE replication there is no cluster membership to
resolve against, so the populated id is simply unused and the primary keeps the
placeholder endpoint, which is coherent.

A stronger variant carries the replica's advertised `host:port` as additional
`REPLCONF` args, so standalone replication also gets real endpoints. That is a
larger wire change; the `NodeId` path is preferred because it reuses an existing
field and the cluster already holds the endpoints. Pick the `host:port` variant
only if standalone per-replica endpoints become a requirement.

### 2. The primary captures each replica per id

Replace the single `slave_offset` (for REPORTING) with a small per-replica table
keyed by `NodeId`:

```text
replicas: NodeId -> { acked: AtomicU64, last_ack: Monotonic, link: AtomicU8 }
```

The per-replica serve task is the SINGLE WRITER of its own entry (ADR-0002), so the
entry's fields stay lock-free atomics. The MAP itself changes only on
attach / detach (cold path), so it sits behind the same `std::sync::Mutex`
cold-path posture the existing `master_endpoint` field already uses (taken only on
attach / detach + the rare `INFO` read, never on the data path or a heartbeat).
`ironcache-repl` is not a lock-lint hot-path crate, so this matches precedent.

The EXISTING `node_offset` / `slave_offset` and the `InSyncReplicas` count remain
the source of truth for the in-sync gate; the new table is additive reporting
state. The in-sync gate keys off the ack/token, never off `node`.

### 3. INFO / CLUSTER SHARDS / topology report per replica

- `INFO # Replication`: one `slaveN:` line PER entry, `ip` / `port` resolved by the
  `node_id_from_announce(member.id) == NodeId` reverse lookup over `SlotMap` members,
  `offset` from the entry's real acked, `state = online` while the link is up, `lag` =
  `head - acked`.
- `CLUSTER SHARDS`: each replica node rendered with its real offset / health.
- `/topology` (#439): the `replication` object gains a `replicas` array of
  `{ id, host, port, offset, lag, link }`.

## Big-O / cost

- Per `INFO` read: O(R + M), R = connected replicas, M = members. Build a
  `NodeId -> (host, port)` map from the members once (O(M), each via
  `node_id_from_announce`), then resolve each replica in O(1) (O(R) total). `INFO` is
  not the data path; this is rare. (A naive per-replica scan would be O(R * M); the
  one-pass map keeps it linear.)
- Per ack: O(1) atomic update of the replica's own entry. The map is sized at
  attach, not per ack.
- ZERO data-hot-path / `bytes_per_key` impact: the status cell is node-level cold
  state (one per node), exactly as ADR-0002 already documents for the existing
  cell. The perf-gate (`bytes_per_key` + qps) should be unchanged.

## Data-safety boundary (CRITICAL)

This is OBSERVABILITY-only. It MUST NOT change: the full-sync / resume decision
(`replica_attach.rs`), the offset tracking that feeds the in-sync gate (ADR-0026),
or the `InSyncReplicas` quorum count. The per-replica table is a NEW reporting
structure; the existing offset / in-sync logic is the source of truth for promotion
and is left byte-unchanged. Populating `Frame::ReplConf.node` only fills an existing
ADVISORY field; the primary's sync behaviour keys off `ack` / `resume_token`, not
`node`, so a wrong / absent `node` degrades reporting (placeholder endpoint), never
correctness.

## Staging (one reviewed + CI-green + perf-gated PR each)

1. **Advertise identity.** The replica populates `Frame::ReplConf.node` with its
   real `NodeId`; a primary-side test asserts it is received. Backward-compatible,
   no reporting change yet.
2. **Capture per replica.** The primary records `(NodeId, acked, last_ack, link)`
   per connected replica in the new table; `INFO` still renders one line but from
   the table. The in-sync gate path is untouched (verified by the existing
   promotion tests).
3. **Report per replica.** `INFO` / `CLUSTER SHARDS` / `/topology` render one entry
   per replica with the resolved endpoint + real offset + lag + health.

Each stage is independently shippable and observable; stage 1 is a pure wire
populate, stage 2 is internal bookkeeping, stage 3 is the visible payoff.

## Compatibility

- **Wire:** `node` is an existing `REPLCONF` field; populating it is
  backward-compatible (it is advisory today, and old peers ignore the value).
- **INFO / CLUSTER SHARDS:** the `slaveN:` line GAINS a real `ip` / `port` (was
  `ip=,port=0`); the field set and order are unchanged (Redis byte-parity). A
  standalone node keeps the placeholder (no membership), which is coherent.
- **Single -> multi replica:** a primary with one replica renders exactly the
  current single `slave0:` line, so existing single-replica deployments and their
  golden tests are byte-unchanged; additional replicas add `slave1:`, `slave2:`,
  etc.
