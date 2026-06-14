# Design specifications

The subsystem design specs that gate implementation. Each builds on the
[Architecture Decision Records](../adr/INDEX.md) (the decisions) and the
[charter](../CHARTER.md) tenets, and is the spec the engine code will implement.
Claim citations in these docs are validated in CI by
[`check-prior-art-claims.sh`](../../scripts/ci/check-prior-art-claims.sh).

- [PROTOCOL.md](PROTOCOL.md): RESP parser, serializer, the HELLO-driven
  per-connection state machine, and the Tier 0 connection commands (#15).
- [ERRORS.md](ERRORS.md): the canonical Redis-compatible error-string catalog
  (#18).
- [HARDENING.md](HARDENING.md): RESP request-size and adversarial-input limits
  (#138).
- [RUNTIME.md](RUNTIME.md): shared-nothing thread-per-core runtime, io_uring
  fast path with portable fallback, and the Env seam (#25).
- [STORAGE_API.md](STORAGE_API.md): the four-primitive narrow-waist storage
  contract (Read/Upsert/Delete/RMW) with hooks (#34).
- [HASHTABLE.md](HASHTABLE.md): the per-shard open-addressing index, growth/
  rehash, and per-entry metadata folding (#35).
- [OBJECT_LAYOUT.md](OBJECT_LAYOUT.md): the one-allocation kvobj (embedded key,
  inline small value, folded metadata bits) (#111).
- [ENCODINGS.md](ENCODINGS.md): compact scalar value encodings (SSO, tagged
  int/float, variable-width header) (#112).
- [EVICTION.md](EVICTION.md): the pluggable EvictionPolicy trait, ghost queue,
  and Redis maxmemory-policy-name mapping (#48, #50).
- [EXPIRATION.md](EXPIRATION.md): per-shard timing-wheel TTL with lazy backstop
  and background reclamation (#51).
- [COMMANDS.md](COMMANDS.md): per-data-type command semantics for strings,
  lists, hashes, sets, sorted sets (arity, flags, *STORE, error edges) (#128).
- [KEYSPACE.md](KEYSPACE.md): generic keyspace commands and the SCAN
  cursor-stability contract, plus DUMP/RESTORE (#129).
- [CLI_BINARY.md](CLI_BINARY.md): the single static binary, its clap
  subcommands, zero-config boot, and self-update (#81).
- [CONFIG.md](CONFIG.md): TOML config, CONFIG GET/SET/REWRITE parity, and live
  reload (#85).
- [OBSERVABILITY.md](OBSERVABILITY.md): native Prometheus, INFO/SLOWLOG/LATENCY
  parity, and the metric/INFO registry (#86, #152).
- [ADMISSION.md](ADMISSION.md): connection admission, maxclients, output-buffer
  limits, and the OOM-write contract (#137).
- [TESTING.md](TESTING.md): the correctness stack (conformance oracle, command
  spec, differential testing, parser fuzzing, property tests, and DST) (#95, #96).
- [BENCHMARK.md](BENCHMARK.md): the reproducible benchmark harness, per-key memory
  model, and the Valkey head-to-head baseline (#8, #96).

## M1: Architecture Specification

Specs added as the M1 milestone progresses.

- [AUTH.md](AUTH.md): the AUTH handshake and credential model (HELLO AUTH, AUTH,
  requirepass, SHA-256 password storage, default user) (#104).
- [COLLECTIONS.md](COLLECTIONS.md): the universal contiguous collection container
  (listpack-equivalent, cascade-update designed out, SIMD-scannable) for small
  list/hash/set/zset, plus the intset-style sorted-array analog (#113). The
  per-shard bucket geometry and rehash policy are an addendum in HASHTABLE.md
  (#110).
- [WTINYLFU.md](WTINYLFU.md): the selectable W-TinyLFU frequency-admission filter
  (4-bit CM-sketch, halving aging, optional doorkeeper off by default,
  admission/eviction-only decision path, incumbent-wins tie-break) as the non-ML
  admission floor (#49).
- [CONNECTION_LIFECYCLE.md](CONNECTION_LIFECYCLE.md): idle-connection `timeout`,
  TCP keepalive for dead-peer detection, and the reaping of dead/wedged/blocked
  connections with per-core resource reclamation (#140).
- [ACL.md](ACL.md): the full ACL engine (SETUSER/GETUSER/categories/key+channel
  patterns/selectors) and aclfile persistence, as an additive superset of the M1
  default user (build deferred, shape specified) (#106).
- [TLS.md](TLS.md): the embedded rustls TLS listener (no C TLS library, cert/key
  config, dedicated tls-port and TLS-only mode, optional mTLS, TLS 1.2/1.3 floor,
  measured plaintext-vs-TLS overhead) (#105).
- [SECRETS.md](SECRETS.md): secrets handling: redaction of secret args from
  SLOWLOG/MONITOR/INFO/logs, zeroize-on-drop, mlock and no-coredump hardening, and
  the MONITOR/metrics auth decision (#145). The shared adversary model is
  [docs/THREAT_MODEL.md](../THREAT_MODEL.md) (#142).
- [DEFRAG.md](DEFRAG.md): online active defragmentation (native slab-sparsity
  query, copy-relocate through the owned per-core index, Redis-compatible throttle
  and thresholds, default off) (#43).
- [PERF_REGRESSION_GATE.md](PERF_REGRESSION_GATE.md): the per-PR performance-
  regression CI gate (micro/macro smoke vs merge-base, stored baselines,
  noise-aware throughput-per-core and bytes-per-key ratchet) (#159).
- [COMPRESSION.md](COMPRESSION.md): transparent value-compression framing
  (codec/dict id, uncompressed length, incompressible flag), single-branch GET
  decode, compressed-bytes maxmemory accounting, per-keyspace opt-in (#52).
- [DICTIONARIES.md](DICTIONARIES.md): off-hot-path ZDICT per-prefix dictionary
  training, monotonic fail-closed dict-version-id, atomic install + lazy
  re-encode (#55).
- [SUPPLY_CHAIN.md](SUPPLY_CHAIN.md): the dependency-vulnerability + license
  merge/release gate (cargo-deny four checks, cargo-audit/RUSTSEC, license
  allow-list, time-boxed exceptions) (#144).
- [PERSISTENCE.md](PERSISTENCE.md): the persistence umbrella: three durability
  tiers with honest loss windows, durable_offset/fsync-lag, fail-closed, shared
  io_uring write path, hybrid-log + segment/manifest layout (#58).
- [CLUSTER_CONTRACT.md](CLUSTER_CONTRACT.md): the Redis-Cluster-compatible client
  wire contract (CRC16/16384 slots, hash tags, CROSSSLOT, MOVED/ASK, CLUSTER
  SLOTS/SHARDS, sharded Pub/Sub) (#70).
- [CONTROL_PLANE.md](CONTROL_PLANE.md): the in-binary Raft control plane owning the
  authoritative slot map, config epoch, membership, and replica promotion (#73).
- [MEMBERSHIP.md](MEMBERSHIP.md): SWIM + non-optional Lifeguard data-plane
  membership and failure detection, joined with the Raft-committed map (#74).
- [REPLICA_READ.md](REPLICA_READ.md): the replica-read contract (READONLY/
  READWRITE, replica routing, bounded staleness surfaced to clients) (#147).
- [NODE_LIFECYCLE.md](NODE_LIFECYCLE.md): cluster bootstrap and node lifecycle
  (seed/MEET join, learner to voter to slot-owner promotion, add/remove-node) (#149).
- [ADVISOR_SAFETY.md](ADVISOR_SAFETY.md): the advisor safety envelope (per-knob
  bounds, hysteresis/cooldown, regression detect + rollback, kill-switch, RCU
  snapshot contract) (#91).
- [ADVISOR.md](ADVISOR.md): the per-shard background advisor (LeCaR/bandit expert
  weighting, bounded knobs, atomic RCU config swap, EvictionPolicy-trait binding,
  shadow/off default per ADR-0013) (#126).
- [ADVISOR_AUDIT.md](ADVISOR_AUDIT.md): the durable tamper-evident advisor
  decision/audit log (knob deltas, trigger, snapshot version, replay evidence,
  rollback/kill events), surfaced via INFO/metrics, emitted even in shadow (#153).
- [ADVISOR_PROMOTION.md](ADVISOR_PROMOTION.md): the offline-replay + shadow-A/B
  promotion gate proving a change beats the static baseline before it acts (#154).
- [ADMIN_COMMANDS.md](ADMIN_COMMANDS.md): the admin/introspection command family
  (CLIENT LIST/INFO/KILL/PAUSE/NO-EVICT/NO-TOUCH with byte-faithful-vs-synthesized
  fields, COMMAND DOCS/INFO/COUNT/GETKEYS, RESET semantics) (#150).
- [RUNTIME_ABSTRACTION.md](RUNTIME_ABSTRACTION.md): the Runtime/IO trait seam
  (owned buffers, monomorphization, Cargo-feature backend select) keeping
  monoio/glommio/tokio swappable (#27).
- [IOURING_DATAPATH.md](IOURING_DATAPATH.md): the Linux io_uring net fast path
  (per-shard ring, registered fixed buffers, multishot + one-shot fallback) (#28).
- [ZSET_LARGE.md](ZSET_LARGE.md): the large sorted-set representation (ordered
  index plus parallel member->score map; final structure deferred to #136) (#134).
- [LIST_LARGE.md](LIST_LARGE.md): the large list (quicklist-equivalent chunked
  listpack deque, O(1) head/tail, ~8KB node sizing) (#135).
- [OBJECT_ENCODING_MAPPING.md](OBJECT_ENCODING_MAPPING.md): the internal-repr to
  OBJECT ENCODING name map and DEBUG OBJECT field synthesis (#40).
- [HYPERLOGLOG.md](HYPERLOGLOG.md): the wire-compatible HyperLogLog (dense P=14 +
  sparse ZERO/XZERO/VAL, PFADD/PFCOUNT/PFMERGE, byte-exact DUMP/RESTORE) (#115).
- [DIFFERENTIAL_TESTING.md](DIFFERENTIAL_TESTING.md): byte-exact RESP differential
  replay vs pinned Valkey/Redis with leading-token error tiering (#97).
- [PROPERTY_TESTING.md](PROPERTY_TESTING.md): proptest+bolero per-type property
  tests with threshold-straddling generators and post-mutation encoding asserts (#98).
- [JEPSEN_PLAN.md](JEPSEN_PLAN.md): Jepsen + Elle consistency test plan (fault
  catalog, async-vs-quorum suites, partition-mid-migration) (#99).
- [COORDINATOR.md](COORDINATOR.md): the per-connection home-thread cross-shard
  coordinator (bounded MPSC fan-out, global txid order, per-shard atomic
  MGET/MSET, back-pressure) (#107).
- [TRANSACTIONS.md](TRANSACTIONS.md): MULTI/EXEC/DISCARD/WATCH with optimistic
  locking, queue-then-apply, dirty-CAS, and no rollback (#19).
- [BLOCKING_COMMANDS.md](BLOCKING_COMMANDS.md): blocking commands (BLPOP family,
  WAIT, XREAD BLOCK) under shared-nothing: per-shard FIFO wait queues, cross-shard
  wakeup, no-block-in-EXEC (#130).
- [SERVER_PUSH.md](SERVER_PUSH.md): the unified server-push channel (Pub/Sub,
  sharded Pub/Sub, keyspace notifications, client-side-caching) and its fan-out
  topology under shared-nothing (#20, #108).
- [CLIENT_TRACKING.md](CLIENT_TRACKING.md): CLIENT TRACKING (BCAST + RESP3 push
  default, bounded global table, RESP2 REDIRECT) (#21).
