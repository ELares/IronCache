# Implementation-readiness roadmap

This roadmap was authored before the engine existed, to prioritize and
sequence the issue tree (163 issues) into the order they should be resolved
and built, derived from the dependency graph and the ranked tenets
(Compatible > Efficient > Simple > Scalable > AI-Driven). The live tracker
reflects it via the **Implementation Readiness** milestone (the critical-path
gate), the `wave:0..3` labels (sequence), and the `critical-path` label (the
thin first slice). The tracking issue is the EPIC-linked companion to this doc.

**Status:** the thin vertical slice and Waves 1 to 3 below are now implemented
(the engine and all core data types, transactions, pub/sub, persistence, security,
and the opt-in Raft cluster), along with later tooling (a separate monitoring
console and a verified data-safe `ironcache upgrade`). This document is retained
as the planning and sequencing record; see the top-level
[README](../README.md) for the current feature set.

## The thin vertical slice (first running binary)

The minimal ordered set to ship the first IronCache binary: boots as one static binary with a CLI/config, speaks RESP over a shared-nothing thread-per-core runtime, stores data in a per-shard single-thread map with a one-allocation per-key layout behind a narrow-waist storage API, serves GET/SET/DEL/EXISTS/EXPIRE/TTL with Redis-faithful replies/errors, enforces a maxmemory ceiling with eviction-on-by-default (a single default policy behind the EvictionPolicy trait) plus TTL expiration, and answers PING/INFO with minimal metrics. Compatibility (correct RESP + error strings + reply shaping) leads; Efficient cleverness (Dash index, compression, io_uring fast path, defrag) is intentionally out of this first slice.

Ordered issues for the slice:

1. #24 — DECISION: shared-nothing thread-per-core concurrency model (the spine everything attaches to)
1. #45 — DECISION: memory ceiling + eviction ON by default (defines the boot-time posture)
1. #41 — DECISION: default allocator + memory-accounting strategy (honest maxmemory)
1. #46 — DECISION: default eviction policy (single concrete default for the slice)
1. #16 — DECISION: compatibility tiering (which commands/replies the slice must be faithful to)
1. #59 — DECISION: durability stance = ephemeral default (lets the slice skip persistence legitimately)
1. #25 — DESIGN: shared-nothing core runtime + async/io stack (the boot + accept loop)
1. #15 — DESIGN: RESP protocol surface + parser (the wire)
1. #18 — DESIGN: Redis-compatible error-string catalog (faithful errors for the core commands)
1. #17 — DESIGN: RESP3 reply-shaping + error-string fidelity policy
1. #34 — DESIGN: narrow-waist storage API (Read/Upsert/Delete/RMW) under the RESP layer
1. #36 — DECISION: per-shard single-thread HashMap as the map (vs concurrent fallback)
1. #35 — DESIGN: hash table + per-key object layout (the actual store)
1. #111 — DESIGN: one-allocation per-key object layout (embedded key + inline small value + metadata bits)
1. #112 — DESIGN: compact scalar value encodings (SSO, tagged int/float) for the string type the slice needs
1. #128 — DESIGN: per-data-type command semantics (string subset: GET/SET/DEL/EXISTS faithful behavior)
1. #129 — DESIGN: generic keyspace semantics (EXISTS/TYPE/DEL/TOUCH contract)
1. #48 — DESIGN: pluggable EvictionPolicy trait (the slice ships one concrete impl behind it)
1. #50 — DESIGN: map Redis maxmemory-policy names onto the engine (so eviction config is compatible)
1. #51 — DESIGN: TTL expiration (per-shard timing wheel + lazy backstop) powering EXPIRE/TTL
1. #137 — DESIGN: connection admission, max-clients, OOM-write/eviction-under-pressure contract
1. #138 — DESIGN: RESP request-size / adversarial-input hardening (parser bounds for a safe first boot)
1. #86 — DESIGN: minimal observability — INFO/PING and a minimal /metrics + INFO field set
1. #152 — DESIGN: native INFO field catalog (the minimal INFO the slice must emit)
1. #81 — DESIGN: single static binary + CLI + single-binary operations
1. #119 — DECISION: CLI mode dispatch (clap subcommands) — concrete CLI shape for the binary
1. #85 — DESIGN: TOML config (subset: bind/port/maxmemory/policy) with CONFIG GET/SET

## Recommended first three implementation PRs

(1) PR-1 'Boot + wire': single static binary skeleton with clap CLI (#81/#119/#82) and TOML config (#85), the thread-per-core runtime + accept loop (#25 over the #24 decision), and the RESP parser/encoder with reply-shaping and the error-string catalog (#15/#17/#18), answering PING and a stubbed INFO (#86/#152) — gated by the Valkey differential harness (#96/#95) on PING/RESP framing. (2) PR-2 'Store + core commands': the narrow-waist storage API (#34), the per-shard single-thread map with one-allocation per-key layout and compact scalar encodings (#35/#36/#110/#111/#112), the allocator + honest memory accounting (#117/#118 implementing #41), and GET/SET/DEL/EXISTS with faithful string/keyspace semantics (#128 string subset, #129) plus parser hardening (#138). (3) PR-3 'Memory ceiling + TTL + eviction': maxmemory accounting wired to the EvictionPolicy trait with the single default policy and W-TinyLFU admission (#48/#49/#46 per the #45 decision), maxmemory-policy name mapping (#50), TTL/EXPIRE via the per-shard timing wheel + lazy backstop (#51), and the connection-admission / OOM-write contract (#137) — turning the binary into a real bounded-memory cache. After these three PRs the thin vertical slice boots, speaks RESP, stores data, evicts under a ceiling, expires keys, and reports INFO/PING as one static binary; Wave 2 then broadens the command/data-type surface and adds opt-in persistence and full security, and Wave 3 adds clustering and the AI advisor.

## Waves

### Wave 0 — Foundational decisions

- **#2** [META]: Scope, ranked tenets, and the five-pillar charter _(after #1)_ — The ranked tenets (Compatible>Efficient>Simple>Scalable>AI) are the tie-breaker for every later trade-off; must be ratified first.
- **#157** [DECISION]: Per-tenet acceptance targets and release gates for Compatible/Efficient/Simple/Scalable/AI-Driven _(after #2, #7, #16, #24)_ — Turns the tenets into measurable gates; defines what 'done/ready' means for each wave.
- **#7** [DECISION]: Headline metrics are throughput-per-core and memory-at-fixed-hit-ratio _(after #2)_ — Defines the success metric all Efficient designs and CI ratchets optimize against.
- **#24** [DECISION]: Shared-nothing thread-per-core as the core concurrency model _(after #2)_ — The architectural spine; map, runtime, eviction, coordinator all attach to it.
- **#31** [DECISION]: Design the runtime for determinism to enable DST _(after #24)_ — Determinism must be designed in from the first runtime line, not retrofitted; gates the testing stack.
- **#16** [DECISION]: Define and publish the IronCache compatibility tiering (Tier 0-4) _(after #2, #10)_ — Compatible is the top tenet; tiering defines exactly which commands/behaviors are in-scope and faithful.
- **#41** [DECISION]: Default global allocator and memory-accounting strategy _(after #2)_ — Honest maxmemory and eviction depend on a settled accounting model and allocator.
- **#45** [DECISION]: Ship a memory ceiling and eviction ON by default _(after #41)_ — Defines default operational posture; the slice must boot with a ceiling and evict.
- **#46** [DECISION]: Default eviction policy (SIEVE vs S3-FIFO vs W-TinyLFU-fronted FIFO) _(after #45, #47)_ — Picks the one concrete default policy the first binary ships; downstream of the eviction bake-off.
- **#59** [DECISION]: Durability stance (ephemeral default, opt-in snapshot, warm-restart, later tiers) _(after #2)_ — Legitimizes shipping a first binary with no persistence; bounds all persistence work as later.
- **#36** [DECISION]: Per-shard single-thread HashMap vs shared concurrent map fallback _(after #24, #33)_ — Decides the core data-structure concurrency posture under thread-per-core.
- **#33** [DECISION]: Epoch-based reclamation (crossbeam-epoch) vs custom drain-list framework _(after #24, #32)_ — Memory-reclamation model for the map/store; foundational for any safe concurrent read path.
- **#69** [DECISION]: Single-node-first roadmap with slot-ready storage layout _(after #2)_ — Explicitly sequences clustering AFTER single-node while keeping storage slot-ready; the non-goal-for-now anchor.
- **#30** [DECISION]: Transaction and scripting surface scope _(after #10, #24)_ — Bounds the protocol surface (no Lua VM); decides what MULTI/EXEC/atomic-ops must do, shaping the command set early.
- **#53** [DECISION]: Default codec = zstd low-level; LZ4 and none as policy options _(after #2)_ — Compression-default decision; locks the codec posture even though compression itself is Wave 2.
- **#155** [DECISION]: Advisor default posture (ship shadow/off by default; engine fully correct and fast with the advisor disabled) _(after #2, #13)_ — Guarantees the engine is fully correct and fast with the AI advisor disabled — keeps AI off the readiness critical path.
- **#146** [DECISION]: Headline scale-out targets (max nodes, slots-per-node working range, rebalance-time and failover-time budgets) _(after #7)_ — A scale-out (Scalable) decision; recorded in Wave 0 as a governance target but explicitly NOT on the single-node path.

### Wave 1 — Core engine + protocol (the thin slice)

- **#25** [DESIGN]: Shared-nothing core runtime and the async/io stack _(after #24, #31)_ — Boot, accept loop, per-core executors; the slice's foundation.
- **#15** [DESIGN]: RESP protocol surface, parser, and compatibility tiers _(after #16)_ — The wire contract; first thing a client touches.
- **#17** [DECISION]: RESP3 reply-shaping policy and error-string fidelity _(after #15)_ — Compatible tenet requires faithful reply shapes and errors for the core commands.
- **#18** [DESIGN]: Redis-compatible error-string catalog _(after #15, #17)_ — Differential tests demand byte-faithful error strings from day one.
- **#34** [DESIGN]: Narrow-waist storage API (Read/Upsert/Delete/RMW) under the RESP layer _(after #25, #33)_ — The seam between RESP and the store; keeps engine swappable behind one API.
- **#35** [DESIGN]: Hash table, data-structure encodings, and per-key object layout _(after #36, #33)_ — The actual in-memory store the commands mutate.
- **#110** [DESIGN]: Per-shard bucket table geometry and incremental rehash policy _(after #35, #36)_ — Concrete table geometry and rehash policy for the map without stalls.
- **#111** [DESIGN]: One-allocation per-key object layout (embedded key + inline small value + metadata bits) _(after #35)_ — Bytes-per-key efficiency for the headline memory metric; defines the key/value cell.
- **#112** [DESIGN]: Compact scalar value encodings (SSO, tagged small int/float, variable-width string header) _(after #35)_ — Efficient string representation needed for GET/SET on day one.
- **#48** [DESIGN]: Pluggable EvictionPolicy trait and ghost queue _(after #46)_ — The trait the slice's single default policy sits behind; keeps eviction swappable.
- **#49** [DESIGN]: W-TinyLFU frequency admission filter (CM-sketch + doorkeeper + aging) _(after #46, #48)_ — The frequency filter fronting the default policy chosen in #46.
- **#50** [DESIGN]: Map Redis maxmemory-policy names onto IronCache's internal engine _(after #45, #46)_ — CONFIG/maxmemory-policy compatibility for the eviction the slice ships.
- **#51** [DESIGN]: TTL expiration via per-shard timing wheel with lazy backstop and background reclamation _(after #33, #35, #46)_ — Powers EXPIRE/TTL and background reclamation in the slice.
- **#128** [DESIGN]: Per-data-type command semantics for strings, lists, hashes, sets, and sorted sets _(after #15, #16, #35)_ — Defines faithful behavior for GET/SET/DEL etc.; Compatible tenet core.
- **#129** [DESIGN]: Generic keyspace commands and SCAN cursor-stability contract (SCAN/HSCAN/SSCAN/ZSCAN, KEYS, TYPE, RANDOMKEY, RENAME, COPY, TOUCH, DUMP/RESTORE) _(after #16, #35)_ — EXISTS/TYPE/DEL/TOUCH and the SCAN contract the slice and tests need.
- **#137** [DESIGN]: Connection admission, max-clients, output-buffer limits, and the OOM-write/DoS contract under sustained pressure _(after #15, #41, #45, #46)_ — Defines behavior under memory pressure and max-clients — a boot-correctness requirement.
- **#138** [DESIGN]: RESP request-size and adversarial-input hardening (proto-max-bulk-len tunable, multibulk count cap, accumulated-frame bound, RESP3 nesting depth, inline-length cap, parser-work budget) _(after #15)_ — Parser bounds so the first listener is not trivially DoS-able; security-by-default.
- **#140** [DESIGN]: Idle-connection timeout, TCP keepalive, and dead-peer reaping _(after #15, #25)_ — Connection lifecycle correctness for a real listener.
- **#104** [DESIGN]: AUTH handshake and credential model (HELLO AUTH, AUTH, requirepass, SHA-256 default user) _(after #22)_ — Minimal auth so the first binary is not open-by-default in any deployed sense.
- **#86** [DESIGN]: Observability (Prometheus /metrics, INFO/SLOWLOG/LATENCY parity) _(after #81)_ — INFO/PING and a minimal metrics surface the slice must answer.
- **#152** [DESIGN]: Metric/label registry, native INFO field catalog, and per-command cardinality bounds _(after #86)_ — Defines the exact minimal INFO fields and metric cardinality bounds.
- **#81** [DESIGN]: Single static binary, CLI, and single-binary operations _(after #24)_ — Simple tenet: the deliverable is one static binary with a CLI.
- **#119** [DECISION]: CLI mode dispatch - clap subcommands vs argv[0] symlink branching _(after #82)_ — Concrete CLI dispatch shape for the binary.
- **#82** [DECISION]: clap subcommands vs argv[0] symlink mode-switching and artifact signing _(after #81)_ — Decides CLI structure and how artifacts are signed; gates the build.
- **#85** [DESIGN]: TOML config with CONFIG GET/SET/REWRITE parity and live reload _(after #81, #22)_ — Config the slice reads at boot (bind/port/maxmemory/policy).
- **#117** [DECISION]: Default global allocator (jemalloc vs mimalloc vs snmalloc) _(after #41, #42)_ — Concrete allocator choice implementing the #41 decision for the binary.
- **#118** [DECISION]: Memory accounting and size-class scheme for honest maxmemory _(after #41)_ — The honest-maxmemory accounting impl behind #41/#45.
- **#8** [DESIGN]: Reproducible benchmark and memory-model harness _(after #7)_ — You cannot land Efficient code without the harness that measures the headline metric.
- **#96** [TASK]: Valkey 9.x as RESP differential-test oracle and head-to-head baseline _(after #8)_ — Differential-test oracle stood up before the first command PR so compat is provable.
- **#95** [DESIGN]: Conformance, differential, fuzz, property, and DST testing stack _(after #31, #96)_ — The test harness that gates every command/protocol PR; Compatible+correctness backbone.

### Wave 2 — Command surface, data types, persistence, ops/security

- **#22** [DESIGN]: Security surface (AUTH, requirepass, ACL, embedded TLS) _(after #15, #104)_ — Full security surface beyond the minimal AUTH of Wave 1.
- **#105** [DESIGN]: Embedded rustls TLS listener (cert/key config, TLS-only mode, no C TLS lib) _(after #22)_ — TLS without a C lib; needed for any real deployment.
- **#106** [DESIGN]: Full ACL engine and aclfile persistence (deferred from M1) _(after #22)_ — Deferred from M1; rounds out the security surface.
- **#142** [DESIGN]: Written threat model (assets, trust boundaries, attacker capabilities, STRIDE per subsystem) _(after #22)_ — Security tenet hygiene before persistence/network features broaden the attack surface.
- **#145** [DESIGN]: Secrets handling: log/MONITOR redaction, in-memory zeroization, and core-dump/swap exposure _(after #22, #86)_ — Prevents credential leakage through logs/MONITOR/core dumps.
- **#144** [TASK]: Continuous dependency-vulnerability and license auditing as a merge/release gate (cargo-audit/RUSTSEC + cargo-deny) _(after #81)_ — cargo-audit/deny as a merge gate; supply-chain hygiene.
- **#19** [DESIGN]: MULTI/EXEC/DISCARD/WATCH with optimistic locking and no rollback _(after #15, #30)_ — Transaction surface per #30; broad client compat.
- **#23** [RESEARCH]: Native atomic-op set covering common scripting use cases without a Lua VM _(after #10, #15, #30)_ — Covers common scripting use cases per the non-goal on Lua.
- **#20** [DESIGN]: Unified server-push channel (Pub/Sub, sharded Pub/Sub, keyspace notifications, CSC) _(after #15)_ — Pub/Sub and client-side-caching push surface.
- **#21** [DESIGN]: CLIENT TRACKING (BCAST + RESP3 push default, per-client table, RESP2 REDIRECT) _(after #15, #20)_ — Client-side caching invalidation; depends on the push channel.
- **#108** [DESIGN]: Pub/Sub fan-out topology under shared-nothing shards _(after #29)_ — Cross-shard fan-out topology for #20.
- **#29** [DESIGN]: Cross-shard coordinator and transaction/scripting surface _(after #19, #24)_ — Multi-key/multi-shard ops (MGET/MSET, txn ordering).
- **#107** [DESIGN]: Cross-shard coordinator: topology, txid ordering, MGET/MSET atomicity, and back-pressure _(after #29)_ — Concrete coordinator mechanics implementing #29.
- **#130** [DESIGN]: Blocking command semantics (BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZMPOP, WAIT, XREAD BLOCK) under the shared-nothing model _(after #15, #19)_ — Blocking ops under shared-nothing; needed for list/zset compat.
- **#113** [DESIGN]: Universal collection container and intset analog (cascade-update designed out) _(after #35, #37)_ — The container backing lists/sets/hashes/zsets.
- **#134** [DESIGN]: Sorted-set (zset) large representation: ordered index plus parallel member->score map _(after #35)_ — zset data type; broad compat.
- **#135** [DESIGN]: List (quicklist-equivalent) representation: linked listpack chunks with O(1) head/tail and node sizing _(after #35)_ — List data type representation.
- **#136** [RESEARCH]: Large-collection structure bake-off (zset ordered index: skiplist vs B-tree/ART; list deque structure) on throughput-per-core and bytes-per-element _(after #7, #35, #37)_ — Picks the zset/list structures by throughput-per-core and bytes-per-element.
- **#37** [RESEARCH]: Adaptive vs fixed encoding-conversion thresholds _(after #35)_ — Listpack->hashtable promotion thresholds for compat encodings.
- **#40** [DESIGN]: OBJECT ENCODING / DEBUG OBJECT compatibility mapping _(after #35)_ — Reports Redis-compatible encodings; differential-test surface.
- **#39** [TASK]: intset and HyperLogLog sparse/dense encodings for wire compatibility _(after #35, #40)_ — Wire-compatible intset/HLL.
- **#114** [TASK]: intset encoding (sorted packed int16/32/64, width upgrade, 512-entry cap, promotion to #35 set path) _(after #35, #39)_ — Concrete intset impl.
- **#115** [TASK]: HyperLogLog encoding (P=14 dense + sparse ZERO/XZERO/VAL, PFADD/PFCOUNT/PFMERGE) _(after #39, #40)_ — Concrete HLL impl.
- **#131** [DESIGN]: Bitmap and BITFIELD semantics over the string type (SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD: addressing, growth, signed/unsigned overflow) _(after #35)_ — SETBIT/BITCOUNT/BITOP/BITFIELD compat.
- **#98** [DESIGN]: Property-based and model-based tests for every data type _(after #35, #95)_ — Correctness gate for each new type.
- **#97** [DESIGN]: Differential testing against pinned redis-server/valkey-server _(after #18, #95, #96)_ — Continuous compat verification across the broadened command set.
- **#158** [DESIGN]: Real client-driver compatibility matrix (run lettuce/redis-py/go-redis/ioredis/node-redis/jedis/StackExchange.Redis own test suites against IronCache) _(after #17, #95, #97)_ — Run lettuce/redis-py/go-redis/etc. own suites against IronCache.
- **#150** [DESIGN]: Admin/introspection command family (CLIENT LIST/INFO/KILL/PAUSE/NO-EVICT/NO-TOUCH, COMMAND DOCS/INFO/COUNT/GETKEYS) _(after #15)_ — CLIENT LIST/KILL/PAUSE and COMMAND DOCS for tooling compat.
- **#151** [DESIGN]: Troubleshooting introspection commands (MEMORY USAGE/STATS/DOCTOR, LATENCY DOCTOR) _(after #86)_ — Operability commands clients/tools expect.
- **#58** [DESIGN]: Persistence, forkless snapshot, and storage-engine architecture _(after #59)_ — The umbrella for opt-in durability the #59 decision allows later.
- **#62** [DESIGN]: mmap warm-restart (graceful shutdown + state file + pointer fixup) _(after #58, #59)_ — Fast restart without full reload; the first persistence feature users want.
- **#139** [DESIGN]: Graceful shutdown contract (SHUTDOWN [NOSAVE|SAVE], SIGTERM/SIGINT, connection drain, optional save-on-exit, orchestrator exit/grace contract) _(after #58, #59, #62)_ — Clean drain + optional save-on-exit; orchestrator contract.
- **#44** [DECISION]: THP and snapshot stance (MADV_NOHUGEPAGE heap, non-fork serialization) _(after #41)_ — Memory posture for snapshotting without fork/THP requirements.
- **#63** [DESIGN]: Segment + atomic manifest durable log with corruption recovery _(after #58)_ — On-disk format for opt-in durability.
- **#43** [DESIGN]: Online defragmentation strategy _(after #41)_ — Bounds RSS drift over long runs; Efficient tenet.
- **#52** [DESIGN]: Transparent value compression strategy _(after #35, #53)_ — Memory efficiency; off the slice's hot path but a headline-metric lever.
- **#54** [DECISION]: C-bound zstd vs pure-Rust zstd for the static binary _(after #53)_ — Build/static-linking implication of the codec choice.
- **#55** [DESIGN]: ZDICT per-prefix dictionary training, versioning, and tagging _(after #52)_ — Improves compression ratio on small values.
- **#56** [DESIGN]: Compression interaction with mutating commands and hot-key cost _(after #52)_ — Hot-key cost of compress/decompress on RMW.
- **#27** [DESIGN]: Runtime/IO abstraction layer keeping monoio/glommio/tokio swappable _(after #25, #26)_ — Keeps the runtime swappable per the bake-off; Efficient flexibility.
- **#28** [DESIGN]: io_uring fast path with registered buffers and multishot ops _(after #25, #27)_ — The Efficient hot-path optimization, after correctness.
- **#38** [DESIGN]: Segmented extendible-hash index (Dash-style) with SIMD fingerprint probing _(after #35)_ — Efficiency upgrade to the map; not needed for first correctness.
- **#141** [DECISION]: Per-command operational latency budget and slow-operation guard under defrag/eviction/snapshot/expire _(after #7, #43, #51)_ — Bounds tail latency under defrag/eviction/snapshot/expire.
- **#159** [DESIGN]: Continuous performance-regression CI gate (per-commit throughput-per-core and bytes-per-key ratchet) _(after #7, #8, #96)_ — Per-commit throughput-per-core and bytes-per-key ratchet.
- **#160** [TASK]: Determinism replay-contract verification as a CI gate (same seed yields byte-identical execution; Env-seam lint against direct nondeterminism) _(after #31, #95)_ — Enforces #31's deterministic execution as a gate.
- **#161** [TASK]: Long-horizon soak and memory-stability correctness gate (no leak, bounded fragmentation/RSS drift, no fd/timer/tracked-key growth) _(after #43, #51)_ — No leak/bounded fragmentation over long runs.
- **#100** [TASK]: Seeded fault-injection and corruption scenarios _(after #31, #63, #95)_ — Validates persistence/recovery correctness.
- **#84** [TASK]: Packaging, cross-build matrix, reproducible builds, SBOM, and musl penalty research _(after #81, #82)_ — Release engineering for the single binary.
- **#121** [TASK]: Reproducible cross-build matrix on cargo-zigbuild (musl x86_64/aarch64 + glibc-pinned gnu fallback) _(after #84)_ — Concrete cross-build impl.
- **#122** [TASK]: Release distribution and install paths (curl|sh installer, Homebrew formula, distroless image, hardened systemd unit) _(after #84)_ — curl|sh, Homebrew, distroless, systemd unit.
- **#123** [TASK]: Supply-chain SBOM and artifact attestation (cargo-auditable embedded SBOM + per-release CycloneDX) _(after #84)_ — CycloneDX + cargo-auditable embedded SBOM.
- **#120** [DECISION]: Release-artifact and self-update signing scheme - minisign vs cosign/sigstore _(after #82)_ — Signing scheme for artifacts and self-update.
- **#83** [DESIGN]: ironcache upgrade with verified rollback _(after #62, #81, #82)_ — Self-update with verified rollback.
- **#133** [DECISION]: Geo command family (GEOADD/GEOSEARCH/GEODIST) scope vs non-goal _(after #16)_ — Decide whether GEO* is in or out before committing data-type work.

### Wave 3 — Later: clustering, AI advisor, tiering, advanced testing

- **#68** [DESIGN]: Single-node to multi-node distribution (partitioning, routing, replication, membership) _(after #69)_ — Scalable tenet; explicitly after the single-node engine works.
- **#70** [DESIGN]: Redis-Cluster-compatible client contract (16384 slots, CRC16, hash tags, MOVED/ASK) _(after #68)_ — Cluster client compat; deferred with clustering.
- **#71** [DECISION]: Internal shard map and partition count decoupled from the 16384 compatibility slots _(after #68, #70)_ — Cluster placement internals.
- **#72** [DECISION]: Keyspace partition count as dual-purpose shard/migration unit _(after #68, #71)_ — Migration unit for rebalancing.
- **#73** [DESIGN]: Raft-managed authoritative slot map and in-binary HA control plane _(after #68, #71)_ — Cluster control plane.
- **#74** [DESIGN]: SWIM + Lifeguard data-plane membership and failure detection _(after #68, #73)_ — Cluster data-plane membership.
- **#75** [DESIGN]: Atomic, snapshot-streamed online slot migration without write freeze _(after #68, #72)_ — Live rebalancing.
- **#76** [DECISION]: Default replication and consistency model (async primary/replica + WAIT) _(after #12, #68)_ — Replication stance.
- **#77** [DESIGN]: Offset-based async replication with adaptive, disk-spillable backlog _(after #68, #76)_ — Replication mechanism.
- **#78** [RESEARCH]: Per-shard Raft for an opt-in strongly-consistent tier _(after #68, #76)_ — Strong-consistency opt-in.
- **#79** [RESEARCH]: Opt-in active-active CRDT mode (reject blanket LWW; principled CRDT/HLC) _(after #12, #68, #76)_ — Active-active; far future.
- **#163** [RESEARCH]: Foundational CRDT literature and OR-Set tombstone GC for the full Redis type surface _(after #79)_ — Research feeding #79.
- **#80** [RESEARCH]: Post-ketama consistent hashing for internal placement _(after #68, #71)_ — Internal placement research.
- **#146** [DECISION]: Headline scale-out targets (max nodes, slots-per-node working range, rebalance-time and failover-time budgets) _(after #68)_ — Governs clustering; decided in Wave 0 but realized here.
- **#147** [DESIGN]: Replica-read contract (READONLY/READWRITE, replica routing, bounded staleness surfaced to clients) _(after #70, #76)_ — Replica routing semantics.
- **#148** [DESIGN]: Rebalancing policy and orchestration (when/which partitions move, hot-slot trigger, node drain and decommission) _(after #75, #80)_ — When/which partitions move.
- **#149** [DESIGN]: Cluster bootstrap and node-lifecycle (seed/MEET join, learner-to-voter-to-slot-owner promotion, add/remove-node surface) _(after #69, #73, #74, #75)_ — Cluster join/lifecycle.
- **#99** [DESIGN]: Jepsen + Elle test plan for clustering/replication _(after #68, #95)_ — Consistency testing for the distributed tier.
- **#64** [DESIGN]: HybridLog storage engine with in-place hot-set updates _(after #58)_ — Advanced storage engine for tiering.
- **#65** [DECISION]: Reject RocksDB/LSM as the core cold engine; choose hybrid-log vs F2 _(after #58, #64)_ — Cold-engine decision; only when tiering is on deck.
- **#66** [DESIGN]: Tiered RAM->SSD value store (extstore-inspired) _(after #58, #64, #65)_ — Tiered storage; post single-node.
- **#67** [DESIGN]: io_uring snapshot/tiering write path with SQPOLL and fallback _(after #28, #58, #66)_ — Tiering I/O path.
- **#60** [DESIGN]: Forkless versioned point-in-time snapshot and diskless full-sync _(after #58, #77)_ — Snapshot for replication full-sync; clustering-adjacent.
- **#61** [RESEARCH]: Bound and enforce snapshot memory overhead; fast parallel restart _(after #58, #60)_ — Snapshot overhead research.
- **#143** [DESIGN]: At-rest encryption of snapshots, warm-restart state, and tiered-store SSD files _(after #59, #60, #63, #66)_ — Encryption for persisted artifacts; after persistence exists.
- **#88** [DESIGN]: AI-driven background advisor (expert selection + bounded knob autotuning) _(after #13, #155)_ — AI-Driven tenet (lowest rank); engine must be correct/fast without it.
- **#126** [DESIGN]: AI-driven background advisor - expert selection + bounded knob autotuning (runtime) _(after #88)_ — Runtime impl of the advisor.
- **#89** [RESEARCH]: Define the advisor objective metric (throughput-per-core/memory, not raw hit ratio) _(after #7, #88)_ — Advisor objective research.
- **#90** [RESEARCH]: Quantify advisor headroom over a tuned W-TinyLFU + SIEVE baseline _(after #47, #88)_ — Justifies the advisor before building it.
- **#91** [DESIGN]: Advisor safety guardrails and mechanism detail (bounded knobs, hysteresis, rollback, kill-switch) _(after #48, #85, #88)_ — Safety mechanism for the advisor.
- **#92** [DESIGN]: Off-path per-value compression decision model _(after #52, #88)_ — AI-assisted compression decisions.
- **#93** [TASK]: Offline Belady-MIN and learned-Belady oracle in the benchmark harness _(after #47, #88)_ — Oracle in the bench harness for the advisor.
- **#153** [DESIGN]: AI advisor observability, explainability, and decision/audit trail (knob from->to, trigger, objective delta, snapshot version, rollback/kill-switch events; surfaced via INFO/metrics and queryable, emitted even in shadow mode) _(after #86, #88, #91)_ — Explainability for advisor decisions.
- **#154** [DESIGN]: Advisor evaluation and promotion gate (offline replay + shadow A/B proving a change beats the static baseline before it acts) _(after #90, #91, #93)_ — Proves a change beats baseline before it acts.
- **#87** [RESEARCH]: Continuously-reported online Belady-MIN gap metric _(after #46, #86)_ — Continuous optimality gap metric.
- **#116** [TASK]: SIMD register-histogram / merge kernels for PFCOUNT, PFMERGE, BITCOUNT (benchmark-gated) _(after #39)_ — Benchmark-gated SIMD optimization; pure Efficient polish.
- **#94** [DESIGN]: AI-assisted development pipeline with adversarial claim verification _(after #4, #6, #88)_ — Build-time AI tooling, not runtime; supports research.
- **#127** [TASK]: Stand up the LLM/agent prior-art mining + adversarial claim-verification pipeline _(after #88)_ — Build-time research tooling.

## Implementation Readiness gate set

These 42 issues must be resolved (decided or implementation-ready) before the
first implementation PR merges. They carry the **Implementation Readiness** milestone:

- #2 — [META]: Scope, ranked tenets, and the five-pillar charter
- #157 — [DECISION]: Per-tenet acceptance targets and release gates for Compatible/Efficient/Simple/Scalable/AI-Driven
- #7 — [DECISION]: Headline metrics are throughput-per-core and memory-at-fixed-hit-ratio
- #24 — [DECISION]: Shared-nothing thread-per-core as the core concurrency model
- #31 — [DECISION]: Design the runtime for determinism to enable DST
- #16 — [DECISION]: Define and publish the IronCache compatibility tiering (Tier 0-4)
- #41 — [DECISION]: Default global allocator and memory-accounting strategy
- #45 — [DECISION]: Ship a memory ceiling and eviction ON by default
- #46 — [DECISION]: Default eviction policy (SIEVE vs S3-FIFO vs W-TinyLFU-fronted FIFO)
- #59 — [DECISION]: Durability stance (ephemeral default, opt-in snapshot, warm-restart, later tiers)
- #36 — [DECISION]: Per-shard single-thread HashMap vs shared concurrent map fallback
- #33 — [DECISION]: Epoch-based reclamation (crossbeam-epoch) vs custom drain-list framework
- #69 — [DECISION]: Single-node-first roadmap with slot-ready storage layout
- #30 — [DECISION]: Transaction and scripting surface scope
- #53 — [DECISION]: Default codec = zstd low-level; LZ4 and none as policy options
- #155 — [DECISION]: Advisor default posture (ship shadow/off by default; engine fully correct and fast with the advisor disabled)
- #25 — [DESIGN]: Shared-nothing core runtime and the async/io stack
- #15 — [DESIGN]: RESP protocol surface, parser, and compatibility tiers
- #17 — [DECISION]: RESP3 reply-shaping policy and error-string fidelity
- #18 — [DESIGN]: Redis-compatible error-string catalog
- #34 — [DESIGN]: Narrow-waist storage API (Read/Upsert/Delete/RMW) under the RESP layer
- #35 — [DESIGN]: Hash table, data-structure encodings, and per-key object layout
- #111 — [DESIGN]: One-allocation per-key object layout (embedded key + inline small value + metadata bits)
- #112 — [DESIGN]: Compact scalar value encodings (SSO, tagged small int/float, variable-width string header)
- #48 — [DESIGN]: Pluggable EvictionPolicy trait and ghost queue
- #50 — [DESIGN]: Map Redis maxmemory-policy names onto IronCache's internal engine
- #51 — [DESIGN]: TTL expiration via per-shard timing wheel with lazy backstop and background reclamation
- #128 — [DESIGN]: Per-data-type command semantics for strings, lists, hashes, sets, and sorted sets
- #129 — [DESIGN]: Generic keyspace commands and SCAN cursor-stability contract (SCAN/HSCAN/SSCAN/ZSCAN, KEYS, TYPE, RANDOMKEY, RENAME, COPY, TOUCH, DUMP/RESTORE)
- #137 — [DESIGN]: Connection admission, max-clients, output-buffer limits, and the OOM-write/DoS contract under sustained pressure
- #138 — [DESIGN]: RESP request-size and adversarial-input hardening (proto-max-bulk-len tunable, multibulk count cap, accumulated-frame bound, RESP3 nesting depth, inline-length cap, parser-work budget)
- #86 — [DESIGN]: Observability (Prometheus /metrics, INFO/SLOWLOG/LATENCY parity)
- #152 — [DESIGN]: Metric/label registry, native INFO field catalog, and per-command cardinality bounds
- #81 — [DESIGN]: Single static binary, CLI, and single-binary operations
- #119 — [DECISION]: CLI mode dispatch - clap subcommands vs argv[0] symlink branching
- #82 — [DECISION]: clap subcommands vs argv[0] symlink mode-switching and artifact signing
- #85 — [DESIGN]: TOML config with CONFIG GET/SET/REWRITE parity and live reload
- #117 — [DECISION]: Default global allocator (jemalloc vs mimalloc vs snmalloc)
- #118 — [DECISION]: Memory accounting and size-class scheme for honest maxmemory
- #8 — [DESIGN]: Reproducible benchmark and memory-model harness
- #96 — [TASK]: Valkey 9.x as RESP differential-test oracle and head-to-head baseline
- #95 — [DESIGN]: Conformance, differential, fuzz, property, and DST testing stack

## Deferred (after the single-node engine works)

- #68 — clustering/distribution umbrella; Scalable tenet, after single-node engine
- #70 — Redis-Cluster client contract; clustering
- #71 — internal shard map; clustering
- #72 — partition/migration unit; clustering
- #73 — Raft slot-map control plane; clustering HA
- #74 — SWIM membership; clustering
- #75 — online slot migration; clustering
- #76 — replication/consistency model; clustering
- #77 — async replication backlog; clustering
- #78 — per-shard Raft strong-consistency tier; clustering
- #79 — active-active CRDT; far-future
- #163 — CRDT literature/OR-Set GC; feeds #79
- #80 — consistent hashing for placement; clustering
- #146 — scale-out targets; governs clustering (decided early, realized late)
- #147 — replica-read contract; clustering
- #148 — rebalancing orchestration; clustering
- #149 — cluster bootstrap/lifecycle; clustering
- #99 — Jepsen/Elle; only meaningful once replication/clustering exists
- #64 — HybridLog engine; tiering, post single-node
- #65 — cold-engine decision; only when tiering is on deck
- #66 — tiered RAM->SSD store; post single-node
- #67 — io_uring tiering write path; tiering
- #60 — forkless snapshot/diskless full-sync; replication-adjacent
- #61 — snapshot overhead bound; snapshot
- #143 — at-rest encryption of persisted artifacts; after persistence
- #88 — AI advisor; lowest-ranked tenet, engine must be correct without it
- #126 — advisor runtime; AI
- #89 — advisor objective metric; AI
- #90 — advisor headroom quantification; AI
- #91 — advisor guardrails; AI
- #92 — off-path compression decision model; AI
- #93 — Belady oracle for advisor; AI
- #153 — advisor observability/audit; AI
- #154 — advisor promotion gate; AI
- #87 — online Belady-MIN gap metric; advisor-adjacent observability
- #94 — AI-assisted dev pipeline; build-time tooling
- #127 — LLM prior-art mining pipeline; build-time tooling
- #116 — SIMD HLL/bitmap kernels; benchmark-gated Efficient polish
- #125 — Windows server binary; explicit deferred non-goal
- #132 — Redis Streams; explicit M0-M2 non-goal
- #10 — committed non-goals register (scripting/Memcached/RDMA/managed runtime); a boundary, not work
- #11 — fork()+COW snapshot, mandatory proxy, host THP tuning non-goal; boundary
- #12 — strong-consistency/zero-write-loss non-goal in async default; boundary
- #13 — per-request neural/ML inference on data path non-goal; boundary
- #14 — no claim without its test/correction; governance boundary
- #101 — no BGSAVE fork-COW non-goal; boundary
- #102 — no host THP/overcommit precondition non-goal; boundary
- #103 — no mandatory hot-path proxy non-goal; boundary
- #156 — advisor adds no runtime net/model/GPU dependency non-goal; boundary
- #162 — second-tier KV landscape research; informs tiering/clustering, not the slice

## Ordering smells to resolve (prerequisite inversions found in issue bodies)

- **#38**: Dash-style segmented hash refs #60 (forkless snapshot, a replication/persistence Wave-3 item). The index design should not be coupled to snapshot mechanics on the critical path. _Fix:_ Build the first map per #35/#110/#36 (single-thread per-shard HashMap); treat #38 as a later Efficient upgrade behind the #34 narrow-waist API and drop the #60 dependency — snapshot-friendliness is a property to validate later, not a design input now.
- **#44**: THP/snapshot stance refs #11 (a non-goal) and #60 (Wave-3 snapshot). It reads as gated on snapshot design. _Fix:_ Split the decision: the THP heap-advice posture (MADV_NOHUGEPAGE) is a Wave-0/1 memory decision tied to #41; the non-fork serialization half can wait for #58/#60 in Wave 2/3.
- **#33**: Epoch reclamation refs #60 and #64 (snapshot/HybridLog, both later). Reclamation must be settled in Wave 0 to build the map, well before those exist. _Fix:_ Decide #33 on the strength of #24/#32 only (single-node read-path safety). Treat #60/#64 as consumers of the reclamation scheme, not inputs to it.
- **#137**: Connection-admission/OOM contract refs #20 and #50 among many; #20 (server-push/CSC) is a Wave-2 item but the OOM-write contract is needed for the first boot. _Fix:_ Land the maxmemory/OOM-write + max-clients core (deps #41/#45/#46) in Wave 1; fold the output-buffer-limit interaction with Pub/Sub (#20) in later when push channels arrive.
- **#51**: TTL expiration refs #33 and #35 (correct) but the timing-wheel + background reclamation can over-reach into snapshot/defrag territory. _Fix:_ Ship the lazy backstop + minimal active-expire cycle for the slice; defer the full timing-wheel tuning and its #43-defrag interaction to Wave 2.
- **#60**: Forkless snapshot is referenced as a dependency by core-engine issues (#33, #38, #44) yet itself sits in the replication/persistence later wave (refs #11 non-goal and #77 replication). _Fix:_ Invert the relationship: snapshot consumes the engine contracts (narrow-waist #34, reclamation #33), so it must sit AFTER them in Wave 3, not be a prerequisite of them. Remove the upstream references to #60 from #33/#38.
- **#29**: Cross-shard coordinator (#29/#107) is referenced by transaction work but pulls in clustering-flavored topology; risk of dragging distribution concerns into single-node. _Fix:_ Scope #29 to single-node multi-shard coordination only (MULTI/EXEC, MGET/MSET atomicity across local shards); explicitly exclude cross-node routing, which belongs to #68.
- **#128**: Per-data-type command semantics is M1 and Compatible-critical, but its refs (#39/#40/#98) pull in HLL/intset/property-tests that are M2. The string subset is needed by the slice while the rest is not. _Fix:_ Split #128: the string/keyspace semantics needed by GET/SET/DEL/EXISTS go in Wave 1; list/set/hash/zset semantics ride with their representations (#134/#135/#113) in Wave 2.
- **#146**: Headline scale-out targets is an M0 decision (so it looks Wave-0) but every dependency (#68/#73/#74/#75/#80) is clustering, a Scalable non-goal-for-now. _Fix:_ Record the targets as governance in Wave 0 but keep #146 off the readiness critical path; its realization belongs in Wave 3 alongside #68.

