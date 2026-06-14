# Research: The second-tier KV/cache landscape (Aerospike, Tarantool, Kvrocks, Hazelcast/Ignite/Coherence, Skytable, Redka)

> Part of the IronCache prior-art research corpus (`docs/research/`). This
> document is DESCRIPTIVE: it records what other systems do, with
> version-pinned claims tracked in [`../prior-art/claims.yaml`](../prior-art/claims.yaml).
> Prescriptive IronCache decisions live in the design issues, not here.
>
> Area: `area:storage` (also touches `area:memory`, `area:replication`, `area:performance`).
> Claims gathered by an AI research agent from primary sources, then load-bearing
> claims independently re-checked by an adversarial verifier.
>
> This is the SECOND prior-art dimension that complements [`../PRIOR_ART.md`](../PRIOR_ART.md)
> and #6. #6's pinned competitor set is exactly Redis / Valkey / Dragonfly / KeyDB /
> Garnet / Memcached. This doc pins, at version, the architectural bets of the
> cache/KV systems that set was missing, and says borrow / adapt / reject per system
> the way [`keydb.md`](keydb.md) does. It feeds #64, #65, #66, #68, and #79. Filed
> from the pre-implementation coverage audit (#162); relates to / partially overlaps #6.

## Summary

The #6 survey pinned the six systems IronCache benchmarks against directly, but it
left out a whole tier of production caches and KV stores whose architectural bets
are load-bearing for IronCache's storage, tiering, and active-active design. This
doc closes that gap for six of them, and two of the gaps are sharp enough to change
how downstream ADRs read.

The first sharp gap is the cold tier. ADR-0023 (#65) rejects RocksDB/LSM as the
primary cold engine, but #6 only ever cites KeyDB FLASH as the Redis-on-RocksDB
precedent [keydb-flash-rocksdb], and KeyDB FLASH is explicitly Beta/experimental
and dormant. The strongest living counter-example to the ADR-0023 rejection is
**Apache Kvrocks**: a maintained, Apache-licensed, distributed KV store that *is*
RocksDB exposed over the Redis protocol, with a proxyless Redis-Cluster-compatible
access path [kvrocks-rocksdb-resp]. Kvrocks proves the rejected option is not a
straw man, it is a real product with real adopters; ADR-0023's rejection therefore
has to stand on the single-static-binary (no C++ toolchain) and SSD-endurance
arguments, not on "nobody ships Redis-on-RocksDB." Citing Kvrocks makes #65 honest.

The second sharp gap is tiering. #66 designs a RAM->SSD value store citing
memcached extstore [extstore-defaults] and FASTER/F2, but never the canonical
hybrid-memory database: **Aerospike**. Aerospike's patented Hybrid Memory
Architecture keeps the primary index in DRAM (64 bytes per record entry) and data
on SSD, read directly from flash on each hit, and its Enterprise all-flash mode
pushes even the index onto flash so a cluster can address billions of records with
a fraction of the DRAM [aerospike-hybrid-memory-index]. That is exactly the
keys/metadata-in-RAM, values-on-flash split #66 is reaching for, plus the
all-flash escape hatch when even the index will not fit. Aerospike's XDR
cross-datacenter active-active also belongs in #79's reading next to Redis
Enterprise CRDB and KeyDB's blanket-LWW anti-pattern.

The other four fill out the design space. **Tarantool** pairs an in-memory engine
(memtx) with an on-disk LSM engine (vinyl) under one fiber-based cooperative
scheduler, and vinyl's key claim is that, because transactions run in a single
dedicated thread, it strips out the locks and inter-thread coordination that
RocksDB pays [tarantool-vinyl-lsm] -- a direct datapoint for IronCache's
thread-per-core thesis (a single-owner LSM can be cheaper than a sharded-lock one).
**Hazelcast / Apache Ignite / Oracle Coherence** are the partitioned in-memory
data-grid lineage: a keyspace split into fixed partitions with configurable
backups, affinity colocation, and a client-side **near-cache** plus read-through /
write-behind to a backing store [ignite-data-grid-near-cache]. The near-cache is
the interesting bet (a second cache in front of the distributed cache) and the
classic invalidation-cost cautionary tale. **Skytable** is a modern Rust NoSQL
DB whose in-memory index is a lock-free concurrent hash trie (`mtchm`,
crossbeam-epoch reclamation, Bagwell/Ctrie lineage) whose own authors warn it
carries heavy memory overhead and they "do NOT recommend its use as a daily data
structure" [skytable-mtchm-index] -- a useful honest reject for IronCache's index
geometry. **Redka** re-implements Redis on top of SQLite (or Postgres),
RESP-compatible, data need not fit in RAM, ACID via SQL transactions, several
times slower than Redis [redka-redis-sqlite] -- the "lean on a mature embedded
engine" point on the spectrum, and the strongest argument for why IronCache builds
its own engine rather than wrapping one.

Net: nothing here overturns an IronCache decision, but three things tighten. #65's
RocksDB rejection gains its missing living counterpoint (Kvrocks). #66's tiering
gains its canonical precedent and its all-flash fallback (Aerospike). #79's
active-active reading gains Aerospike XDR. And Tarantool vinyl, Skytable mtchm,
and Redka each contribute one calibration datapoint for #64's engine and index.

## Mechanisms: borrow, adapt, or reject

| Mechanism | System | Stance | What it does | Rationale for IronCache |
| --- | --- | --- | --- | --- |
| Hybrid Memory Architecture: primary index in DRAM, data on SSD, read direct from flash | Aerospike | **borrow** | Keeps the primary index entirely in DRAM (64-byte per-record entry) and stores record data only on SSD, reading it directly from the device on each access; storage model is selectable per namespace (all-in-memory, index-in-RAM/data-on-flash, or all-flash) [aerospike-hybrid-memory-index]. | This is the canonical version of exactly what #66 specifies (keys + compact metadata in RAM, values on flash) and what #6 never cited. Borrow the index-in-RAM/values-on-flash split and the per-namespace selectability (IronCache's per-keyspace tiering). The 64-byte index entry is a concrete budget to beat: IronCache's one-allocation kvobj (#111) plus a [page,offset,version] pointer (#66) should land at or below it. |
| All-flash mode: index itself on flash when DRAM will not hold it | Aerospike (Enterprise) | **adapt** | When even the in-DRAM index is too large, the index is moved onto flash so a cluster addresses billions of records with a small fraction of the DRAM the hybrid-memory mode would need [aerospike-hybrid-memory-index]. | The escape hatch #66 lacks: what happens when keys+metadata exceed RAM. Adapt as a future mode, not a default; it trades a guaranteed extra flash read on the index path for capacity. Records the design boundary: IronCache's default is index-in-RAM, with an all-flash index as an opt-in capacity tier, gated behind the #66 value store landing first. |
| XDR cross-datacenter active-active replication | Aerospike | **adapt** | Asynchronous cross-datacenter replication supporting active-active topologies for geo distribution. | Belongs in #79's reading alongside Redis Enterprise CRDB and KeyDB. Adapt the async-geo shape but, per #79, reject any blanket last-write-wins conflict model; IronCache's active-active must be per-type CRDT / HLC, correct by construction. Aerospike is the production datapoint that async geo active-active is operable at scale. |
| RocksDB exposed over the Redis protocol (RESP2/3), proxyless Redis-Cluster-compatible | Apache Kvrocks | **reject** | A distributed KV store that uses RocksDB as its storage engine and speaks the Redis protocol, encoding all Redis types into RocksDB column families (metadata, subkey, zset-score, pubsub, propagate), with a proxyless centralized cluster that Redis Cluster clients can talk to [kvrocks-rocksdb-resp]. | This is the living, maintained counter-example to ADR-0023 (#65): Redis-on-RocksDB as a whole product, not a feature. Reject for IronCache for the ADR-0023 reasons: the C++ RocksDB toolchain breaks the single static binary (Compatible tenet) and leveled-compaction write amplification plus compaction stalls hurt SSD endurance and tail latency (Efficient tenet). But CITE it: #65 must reject the option that Kvrocks proves is real, on the binary-shape and endurance arguments, not on novelty. Borrow only the column-family separation idea (data vs metadata vs expires), already noted from KeyDB FLASH. |
| memtx + vinyl: one in-memory engine and one on-disk LSM under a fiber scheduler | Tarantool | **adapt** | Two storage engines selectable per space: memtx (in-RAM) and vinyl (on-disk LSM). Vinyl removes the locks/IPC that general LSMs like RocksDB pay by exploiting that all transactions run in a single dedicated thread [tarantool-vinyl-lsm]; the runtime uses cooperative fibers, and a transaction commit yields to write the WAL. | Two datapoints for IronCache. First, the single-owner-thread-removes-locks insight directly supports the shared-nothing thread-per-core thesis (ADR-0002): a per-shard LSM owned by one core can be cheaper than a globally-shared, lock-mediated one, relevant if the #65 lean-Rust-LSM fallback is ever built. Second, fibers-as-cooperative-tasks is the same lane as IronCache's async runtime (#25); adapt the per-engine-per-space selectability into IronCache's per-keyspace tiering. Reject vinyl wholesale as a primary engine (LSM, per ADR-0023). |
| Partitioned in-memory data grid with backups + affinity colocation | Hazelcast / Apache Ignite / Oracle Coherence | **adapt** | Keyspace split into a fixed number of partitions distributed across nodes with N configurable backup copies; an affinity function colocates related keys on the same partition/node to keep multi-key ops and compute local [ignite-data-grid-near-cache]. | The data-grid partition+backup+affinity model is conceptually IronCache's slot map (#71) plus replication (#76) plus hash-tag colocation (#70). Adapt: IronCache already commits to Redis-Cluster-compatible 16384 hash slots (ADR-0025) over a clean partition count, so borrow the affinity-colocation idea (= hash tags) and the configurable-backups idea (= replica factor), but keep the Redis wire contract rather than the grid's bespoke client API. |
| Client-side near-cache + read-through / write-behind to a backing store | Hazelcast / Ignite / Coherence | **reject (near-cache)** / **adapt (read-through)** | A near-cache is a second, local cache on the client in front of the distributed cache for read-heavy keys; read-through loads a miss from a backing store on demand, write-behind asynchronously flushes writes to it [ignite-data-grid-near-cache]. | Reject the near-cache as an IronCache feature: it pushes a coherence/invalidation problem onto every client and is the classic stale-read footgun; IronCache is the cache, not a tier behind another cache. Read-through/write-behind to a backing store is the cache-aside pattern and is an APPLICATION concern, explicitly outside IronCache's contract (it is a Redis-compatible cache, not an ORM). Note it only to draw the boundary. |
| Lock-free concurrent hash trie index (`mtchm`) | Skytable | **reject** | An in-memory lock-free concurrent hash-trie map (Bagwell / Ctrie lineage, crossbeam-epoch reclamation, tagged atomic pointers) used as the primary index; the authors note it uses full-sized nodes for performance, carries significant memory overhead, and explicitly "do NOT recommend its use as a daily data structure" [skytable-mtchm-index]. | A direct calibration point for #35 (the per-shard index). Reject the hash-trie geometry: IronCache's shared-nothing model (one core owns a shard) means the index does NOT need lock-free concurrency at all; single-owner per-shard open-addressing (#35) avoids the trie's pointer-chasing and the memory overhead its own authors flag. Borrow only the crossbeam-epoch reclamation idea where cross-shard structures are unavoidable (already the plan via ADR-0004). Skytable is the cautionary case for paying for concurrency the architecture removes. |
| Redis re-implemented on SQLite/Postgres, RESP-compatible, larger-than-RAM, ACID | Redka | **reject** | Re-implements core Redis on top of SQLite (or Postgres): RESP wire protocol, the five core types, data need not fit in RAM, ACID transactions via the SQL engine, SQL views for introspection; reported several times slower than Redis (up to ~100K ops/sec on a laptop) [redka-redis-sqlite]. | The "wrap a mature embedded engine" end of the spectrum, and the argument for why IronCache does NOT do that. Reject: leaning on SQLite buys ACID and larger-than-RAM cheaply but pays a multiple of Redis latency, which violates IronCache's max-throughput-per-core thesis outright. Borrow exactly one idea: SQL/queryable VIEWS over the keyspace for introspection are a genuinely nice operability touch; note for the observability surface (#86) as an optional read-only export, never on the hot path. |

## Implications for IronCache

- Cite **Kvrocks** in ADR-0023 / #65 as the living Redis-on-RocksDB counter-example, so the RocksDB rejection rests on the single-static-binary (no C++ toolchain) and SSD-endurance arguments, not on the false claim that nobody ships Redis-on-RocksDB. Kvrocks is maintained and Apache-licensed [kvrocks-rocksdb-resp], unlike the dormant KeyDB FLASH precedent #6 currently leans on.
- Cite **Aerospike Hybrid Memory** in #66 as the canonical index-in-RAM/values-on-flash precedent #6 never had, and use its 64-byte index entry as the budget IronCache's kvobj (#111) + [page,offset,version] pointer (#66) must beat [aerospike-hybrid-memory-index].
- Record Aerospike **all-flash** as the #66 capacity escape hatch (index on flash when DRAM will not hold it), an opt-in mode gated behind the value store landing, not a default.
- Add Aerospike **XDR** to #79's active-active reading as the production datapoint that async geo active-active is operable, while still rejecting blanket LWW for per-type CRDT/HLC.
- Use **Tarantool vinyl**'s single-owner-thread-removes-locks claim [tarantool-vinyl-lsm] as supporting evidence for ADR-0002 (shared-nothing thread-per-core) and for the #65 lean-Rust-LSM fallback shape, should it ever be built.
- Treat the **data-grid** partition+backup+affinity model (Ignite/Hazelcast/Coherence) [ignite-data-grid-near-cache] as conceptual prior art for the slot map (#71) + replicas (#76) + hash-tag colocation (#70), but keep the Redis wire contract over a bespoke grid client API; reject the client near-cache as a stale-read footgun and keep read-through/write-behind an application concern.
- Use **Skytable mtchm** [skytable-mtchm-index] as the cautionary case for #35: IronCache's single-owner-per-shard index does not need lock-free concurrency, so it should avoid the hash-trie's overhead its own authors warn against, and reserve crossbeam-epoch (ADR-0004) only for unavoidable cross-shard structures.
- Use **Redka** [redka-redis-sqlite] as the argument for building IronCache's own engine rather than wrapping SQLite (the latency multiple is disqualifying), while flagging its SQL-views-over-the-keyspace idea as an optional, off-hot-path introspection feature for #86.
- None of these six changes a frozen decision. They make #65 honest, give #66 and #79 their missing precedents, and supply #64/#35 three calibration datapoints. Track all six as historical/secondary prior art, not as head-to-head benchmark baselines (Valkey remains the conformance oracle and benchmark baseline per #6).

## Key claims

Load-bearing claims are marked. The `id` cross-references `claims.yaml`.

| id | system | version | value | conf. | check |
| --- | --- | --- | --- | --- | --- |
| `aerospike-hybrid-memory-index` * | Aerospike | Database 7 (2024); EE for all-flash | Hybrid Memory: primary index in DRAM (64 B/record entry), data on SSD read direct from flash; all-flash mode (EE) puts the index on flash too; storage model selectable per namespace | high | verified |
| `kvrocks-rocksdb-resp` * | Apache Kvrocks | 2.15.0 (2026-02-27), Apache-2.0 | Distributed KV on RocksDB, Redis-protocol (RESP2/3) compatible; types encoded into RocksDB column families; proxyless Redis-Cluster-compatible access | high | verified |
| `tarantool-vinyl-lsm` * | Tarantool | docs (latest, read 2026-06-13) | Two engines: memtx (in-RAM) + vinyl (on-disk LSM); vinyl removes locks/IPC that RocksDB pays by running all txns in one dedicated thread; fiber cooperative scheduling, WAL on commit-yield | medium | verified |
| `ignite-data-grid-near-cache` * | Apache Ignite / Hazelcast / Coherence | Ignite docs (latest, read 2026-06-13) | Partitioned in-memory grid: fixed partitions, N configurable backups, affinity colocation by key; client-side near-cache + read-through/write-behind to a backing store | medium | verified |
| `skytable-mtchm-index` * | Skytable | 0.8.4 (2024-08-07), AGPL-3.0 | In-memory primary index is a lock-free concurrent hash trie (`mtchm`, Bagwell/Ctrie lineage, crossbeam-epoch); authors note heavy memory overhead, "do NOT recommend its use as a daily data structure" | medium | verified |
| `redka-redis-sqlite` * | Redka | nalgeon/redka, near-1.0 (read 2026-06-13) | Redis re-implemented on SQLite/Postgres; RESP wire + 5 core types; data need not fit in RAM; ACID via SQL txns; SQL views for introspection; several times slower than Redis (~100K ops/sec on a laptop) | medium | verified |

`*` = load-bearing. `check`: result of the independent adversarial re-verification.

Claims reused from other dimensions (already pinned): `keydb-flash-rocksdb`, `keydb-flash-config`, `keydb-flash-beta`, `keydb-flash-190gb-benchmark` (keydb); `extstore-defaults` (memcached); `redis-crdb-datatype-mapping`, `keydb-active-replica-lww`, `keydb-multimaster-lww-undefined` (redis-replication-cluster / keydb, via #79).

## Research papers and primary sources

- **Aerospike: Architecture of a Real-Time Operational DBMS** (VLDB 2016, Srinivasan et al.). The hybrid-memory design: index in DRAM, data on flash, direct-from-device reads, and the cluster/clustering model. [source](https://www.vldb.org/pvldb/vol9/p1389-srinivasan.pdf) Relevance: the canonical hybrid-memory precedent #66 was missing; index-in-RAM/values-on-flash and the all-flash escape hatch.
- **Aerospike Hybrid Memory / Flexible storage docs** (Database 7, read 2026-06-13). Per-namespace storage models (all-in-memory / hybrid / all-flash), 64-byte index entry, in-memory compression added in 7.0. [source](https://aerospike.com/docs/database/learn/architecture/hybrid-storage) Relevance: pins `aerospike-hybrid-memory-index`.
- **How we use RocksDB in Kvrocks** (Apache Kvrocks blog/wiki, read 2026-06-13). Column-family layout (metadata / subkey / zset-score / pubsub / propagate), key encoding, and the RocksDB-as-RESP design. [source](https://kvrocks.apache.org/blog/how-we-use-rocksdb-in-kvrocks/) Relevance: the living counter-example to ADR-0023; pins `kvrocks-rocksdb-resp`.
- **The Bw-Tree / log-structured and LSM storage lineage** and **WiscKey: Separating Keys from Values** (FAST 2016, Lu et al.). Key/value separation to cut LSM write amplification on SSD. [source](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu) Relevance: frames why Kvrocks-on-RocksDB (leveled LSM) is rejected for IronCache's endurance goal vs a keys-in-RAM/values-on-flash tier.
- **Tarantool vinyl storage engine docs** (read 2026-06-13). Vinyl as an LSM that drops locks/IPC by running transactions in a single dedicated thread; fibers and WAL-on-commit. [source](https://www.tarantool.io/en/doc/latest/platform/engines/vinyl/) Relevance: supports the single-owner-thread thesis (ADR-0002); pins `tarantool-vinyl-lsm`.
- **Concurrent Tries with Efficient Non-Blocking Snapshots** (Prokopec et al., PPoPP 2012) and **Phil Bagwell, Ideal Hash Trees**. The Ctrie/HAMT lineage Skytable's `mtchm` borrows from. [source](https://aleksandar-prokopec.com/resources/docs/ctries-snapshot.pdf) Relevance: pins the design lineage and the memory-overhead caveat behind `skytable-mtchm-index`.
- **Apache Ignite Data Partitioning / Affinity Colocation docs** (read 2026-06-13). Partition count, backups, affinity functions, near-cache, read-through/write-behind. [source](https://ignite.apache.org/docs/latest/data-modeling/data-partitioning) Relevance: pins `ignite-data-grid-near-cache`; conceptual prior art for #70/#71/#76.

## Open questions

- Aerospike pins its in-DRAM index entry at 64 bytes per record; what is IronCache's true per-key RAM cost in the #66 tiered mode (kvobj header #111 + [page,offset,version] pointer), and does it beat 64 bytes at the same durability?
- Kvrocks is the maintained Redis-on-RocksDB product (2.15.0, 2026-02): does it publish independent write-amplification / compaction-stall numbers on cache-grade churn that quantify ADR-0023's endurance argument, or must IronCache measure them itself?
- Tarantool vinyl claims single-dedicated-thread execution lets it drop the locks RocksDB pays; does that advantage survive at IronCache's per-core sharding granularity, and does it change the #65 lean-Rust-LSM-fallback calculus at all?
- Skytable's authors warn mtchm carries heavy memory overhead and is not for daily use; is there any cross-shard IronCache structure (the slot map? cluster bus?) where a Ctrie-style lock-free trie is actually warranted, or is single-owner-per-shard + epoch GC always sufficient?
- Redka is several-times-slower than Redis on SQLite; is there ANY IronCache surface (cold archival? a queryable export?) where wrapping a mature embedded engine is acceptable because the hot path does not touch it?
- Data grids expose a client-side near-cache; does any IronCache client-library story (RESP client) risk reintroducing the near-cache stale-read problem by default, and should the docs explicitly warn against client-side caching layered on IronCache?

## Proposed issues (seeds for the tracker)

- **[task, M1]** Task: add a Kvrocks citation to ADR-0023 / #65. Insert Kvrocks as the maintained, Apache-licensed Redis-on-RocksDB counter-example [kvrocks-rocksdb-resp] so the RocksDB rejection rests on single-static-binary + endurance, not novelty.
- **[task, M2]** Task: add an Aerospike Hybrid Memory citation to #66. Cite the index-in-RAM/values-on-flash precedent and the 64-byte index entry [aerospike-hybrid-memory-index] as the per-key budget to beat.
- **[design, M2]** Design: an all-flash (index-on-flash) capacity tier for #66. Specify an opt-in mode that moves the index to flash when DRAM cannot hold it (Aerospike all-flash), gated behind the value store landing; quantify the extra-flash-read-per-op cost.
- **[research, M2]** Research: add Aerospike XDR to #79's active-active reading. Use XDR as the production datapoint for async geo active-active while still rejecting blanket LWW for per-type CRDT/HLC.
- **[research, M1]** Research: Tarantool vinyl single-owner-thread LSM as evidence for ADR-0002 and the #65 fallback. Capture the locks-removed-by-single-thread claim [tarantool-vinyl-lsm] as supporting evidence for shared-nothing thread-per-core and the lean-Rust-LSM fallback shape.
- **[non-goal, M1]** Non-goal: client-side near-cache and read-through/write-behind. Declare the data-grid near-cache (a cache in front of the cache) and ORM-style read-through/write-behind explicitly out of IronCache's contract [ignite-data-grid-near-cache]; warn against layering client caches on IronCache.
- **[research, M1]** Research: pin the index-overhead lesson from Skytable mtchm against #35. Use Skytable's own "not recommended as a daily data structure" caveat [skytable-mtchm-index] to justify single-owner-per-shard open addressing over a lock-free trie.
- **[research, M0]** Research: pin Redka as the why-not-wrap-SQLite argument. Record the SQLite-backed Redis re-implementation and its latency multiple [redka-redis-sqlite] as the case for IronCache's own engine; flag SQL-views-over-keyspace as an optional #86 introspection idea.
- **[task, M1]** Task: register `second-tier-kv` as a research dimension. Add the doc to docs/research/README.md and a docs/research/corpus.json entry (schema: dimension, summary, prior_art_claims, mechanisms, ironcache_implications, research_papers, open_questions, proposed_issues, verify_notes) so the six new claims are discoverable alongside the #6 set.
