# Design: Redis-Cluster-compatible client wire contract

Issue: #70. Decisions: ADR-0025 (internal partition count decoupled from the
16384 compatibility slots, #72/#71), ADR-0011 (single-node-first, slot-ready
layout), ADR-0012 (scale-out targets). Related: #71 (internal shard map), #11
(snapshot mechanics), #147 (replica-read routing, PR-20), #68 (clustering parent).

## Goal and scope

IronCache must look like a Redis Cluster to any unmodified client that already
speaks the protocol, so an off-the-shelf redis-cli, go-redis, lettuce, or ioredis
routes reads and writes without changes. This spec fixes the client-visible wire
contract only: the 16384-slot space, CRC16/XMODEM slotting, hash-tag co-location,
the CROSSSLOT rejection, MOVED/ASK redirection, the CLUSTER SLOTS / CLUSTER SHARDS
topology projection, and sharded Pub/Sub slot routing. It renders the partition
model committed in ADR-0025: the internal shard/migration unit (#71/#72) is owned
there and may be finer than 16384; this layer translates it up to the fixed
client view. Out of scope: the internal shard map (#71), snapshot/migration
mechanics (#11), membership and consensus (#73/#74, PR-19), and replica-read
routing, which is split to #147 (PR-20).

## Design

### Slot space and hashing

- The wire slot space is exactly 16384 slots [redis-cluster-16384-slots], adopted
  verbatim; a key's slot is CRC16(key) mod 16384 using the XMODEM CRC16 variant
  (poly 0x1021, init 0x0000, no reflection, no output XOR)
  [redis-cluster-crc16-xmodem]. The bitwise form CRC16(key) & 0x3FFF is identical
  [redis-cluster-hash-slots-16384]. Any other hash desyncs every client, so no
  alternative is offered. The 16384 count is kept on the wire even when the
  internal partition map is finer (ADR-0025); 16384 was chosen upstream to keep
  the gossip served-slots bitmap at 2 KB [redis-cluster-why-16384], and IronCache
  keeps the number for client compatibility, not for that gossip reason.

### Hash tags and CROSSSLOT

- Hash tags co-locate keys: only the substring between the first `{` and the
  first following `}` is hashed, and only when at least one character lies between
  them [redis-cluster-hash-tags][redis-cluster-hash-tag-rule]. The fallbacks are
  adopted exactly: a missing brace, or an empty `{}` with nothing between the
  braces, hashes the whole key (so `foo{}` and `foo` hash by the whole key, while
  `{user1000}.following` and `{user1000}.followers` share a slot). A multi-key
  command whose keys do not all resolve to one slot is rejected with the CROSSSLOT
  error rather than scattered [redis-cluster-hash-tags]; best-effort scatter is
  rejected because it violates the client's atomicity expectation. The CROSSSLOT
  string is drawn from the canonical error catalog (ERRORS.md).

### MOVED / ASK redirection

- Redirection is client-side. A node returns MOVED <slot> <host:port> when a slot
  is permanently relocated, and the client updates its cached slot map; it returns
  ASK <slot> <host:port> during an in-flight move, and the client retries only
  that one query at the target after sending ASKING, without updating its map
  [redis-cluster-moved-ask]. MOVED, ASK, and CROSSSLOT are RESP error frames
  carried over the wire framing in PROTOCOL.md. Proxy-side rerouting that hides
  moves is rejected: it defeats the client-side slot cache the contract feeds.

### CLUSTER SLOTS / CLUSTER SHARDS projection

- CLUSTER SLOTS and CLUSTER SHARDS are computed on demand from internal state, not
  stored as a second source of truth. The translation layer coalesces contiguous
  internal partitions (ADR-0025) into the 16384-slot ranges the client expects, so
  the projection always renders the full 16384-slot space regardless of internal
  granularity. Topology is projected over SWIM membership (#74), whose per-member
  load and detection time are independent of cluster size [swim-scalability],
  rather than running a second Redis-style gossip bus that would duplicate state.
  Replica role/health fields in CLUSTER SHARDS depend on the replication model,
  which serves replica reads via asynchronous replication
  [redis-cluster-async-replication]; the read-routing half is specified in #147.

### Sharded Pub/Sub routing

- SPUBLISH and SSUBSCRIBE route by the same CRC16/XMODEM slotting as keys
  [redis-cluster-crc16-xmodem], so a channel and a same-named key resolve to one
  shard. Sharded Pub/Sub confines a message to its owning shard rather than
  broadcasting cluster-wide [sharded-pubsub-7.0], reusing the server-push channel;
  channel-to-shard and key-to-shard routing therefore agree by construction.

## Open questions

- Whether IronCache emits ASK during purely internal resharding, or only on
  externally visible slot moves (internal moves may stay invisible to clients).
- How SWIM member states map onto the Redis CLUSTER SHARDS `health` values
  (online/loading/failed), shared with the membership design (#74).
- Whether the legacy CLUSTER NODES text format is required for older clients, or
  CLUSTER SLOTS / SHARDS suffices; default is SLOTS/SHARDS-only unless a target
  client forces NODES.

## Slice-1 scope and the single-node divergence

Slice 1 ships the client-visible `CLUSTER` command surface and the CRC16/XMODEM
slot projection, gated on `cluster-enabled` exactly like Redis's `clusterCommand`:

- With `cluster-enabled no` (the slice-1 default) a real Redis rejects EVERY
  `CLUSTER` subcommand at the top of `clusterCommand` with
  `-ERR This instance has cluster support disabled` (the
  `server.cluster_enabled == 0` gate). There is NO per-subcommand carve-out: even
  KEYSLOT / INFO / SLOTS are rejected. IronCache matches this byte-for-byte.
- With `cluster-enabled yes` IronCache behaves as a SINGLE-NODE cluster that
  AUTO-OWNS all 16384 slots: CLUSTER INFO reports `cluster_enabled:1`,
  `cluster_slots_assigned:16384`, `cluster_size:1`; CLUSTER SLOTS / SHARDS / NODES
  render one `0-16383` range owned by self. The topology-mutation subcommands
  (MEET / ADDSLOTS / SETSLOT / DELSLOTS / FORGET / REPLICATE / FAILOVER / RESET /
  BUMPEPOCH / FLUSHSLOTS / SET-CONFIG-EPOCH) return
  `-ERR <SUBCOMMAND> is not supported on a single-node cluster`.

This single-node auto-slots behavior is the ONE deliberate divergence from Redis:
a fresh real-Redis cluster-enabled node owns NO slots until `CLUSTER ADDSLOTS`,
whereas an enabled IronCache node owns all 16384 immediately (so an unmodified
client routes to it without a slot-assignment step). Multi-node slot assignment,
`CLUSTER ADDSLOTS`, MOVED/ASK redirection, and CROSSSLOT enforcement arrive in
slice 2; everything else in this slice matches Redis. CLUSTER COUNTKEYSINSLOT and
GETKEYSINSLOT are bounds-validated but return a documented placeholder (0 / empty)
because an accurate per-slot count needs the cross-shard slot index built in slice
2. CLUSTER INFO and CLUSTER NODES are RESP3 verbatim (`txt`) strings, matching
Redis's `addReplyVerbatim`.

## Acceptance and test hooks

- CRC16/XMODEM slot assignment matches a reference Redis Cluster bit-for-bit
  across a key corpus, including hash-tagged and empty-/missing-brace keys.
- With `cluster-enabled no`, every CLUSTER subcommand (including KEYSLOT/INFO/
  SLOTS) returns `-ERR This instance has cluster support disabled`; with
  `cluster-enabled yes`, the introspection subcommands reply with the single-node
  (all-16384-slots) projection and the topology-mutation subcommands return the
  single-node "not supported" error. This is the one documented divergence (above).
- Unmodified redis-cli, go-redis, lettuce, and ioredis route reads and writes with
  no errors against a running IronCache.
- A cross-slot multi-key command returns CROSSSLOT; a hash-tag-co-located one
  succeeds, matching the oracle (TESTING.md).
- MOVED and ASK responses drive correct client map refresh and one-shot ASKING
  retry, verified differentially against a reference cluster.
- CLUSTER SLOTS / CLUSTER SHARDS always render exactly 16384 slots regardless of
  the internal partition granularity (ADR-0025).
- SPUBLISH to a channel reaches an SSUBSCRIBE on the channel's owning shard only.

## References

- ADR-0025, ADR-0011, ADR-0012; issues #70, #71, #72, #11, #147, #73, #74, #68;
  specs TESTING.md, PROTOCOL.md, ERRORS.md.
- Claims: [redis-cluster-16384-slots], [redis-cluster-crc16-xmodem],
  [redis-cluster-hash-slots-16384], [redis-cluster-hash-tags],
  [redis-cluster-hash-tag-rule], [redis-cluster-moved-ask],
  [redis-cluster-why-16384], [redis-cluster-async-replication],
  [swim-scalability], [sharded-pubsub-7.0].
