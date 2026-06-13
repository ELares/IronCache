# Prior art: how the world caches, and what IronCache takes from it

This survey is the comparative reading that grounds IronCache. It is
**descriptive**: it records what other systems do at a pinned version, never
what IronCache should do. Every load-bearing numeric or version-specific
statement carries a `[<id>]` tag that names an entry in
[`prior-art/claims.yaml`](prior-art/claims.yaml), which is the single source of
truth; when the prose and the pinned file disagree, the file wins. The
per-dimension detail, including mechanisms tables and proposed issues, lives in
[`research/`](research/).

A note on provenance and honesty: these claims were gathered by a fan-out of AI
research agents and then re-checked by independent adversarial verifiers. The
verification caught real errors, and the corrected values are what appear below
and in `claims.yaml`. The notable corrections are called out in their own
section at the end so a reviewer can re-verify them first.

## The landscape at a glance

| System | License | Concurrency of command execution | Memory story | Persistence | Multi-node | Distribution shape |
| --- | --- | --- | --- | --- | --- | --- |
| Redis OSS | SSPLv1 / RSALv2 (2024 relicense) | Single-threaded core, I/O offload only | Approximated LRU/LFU, listpack encodings, jemalloc | RDB fork snapshot + AOF | Redis Cluster (16384 slots) | 6 separate binaries |
| Valkey | BSD-3-Clause | Single-threaded core, richer async I/O threading | New open-addressing hashtable, per-slot dicts, embedded keys | RDB + AOF | Cluster + atomic slot migration | Multiple binaries |
| KeyDB | BSD-3-Clause | Multi-thread shared keyspace under a spinlock | Redis 6 lineage + FLASH (RocksDB) tier | RDB + AOF, MVCC snapshots | Active-replica multi-master | Fork of Redis |
| DragonflyDB | BSL 1.1 | Shared-nothing thread-per-core, no hot-path locks | Dashtable (low metadata), mimalloc | Forkless versioned snapshot | Emulated then real cluster | Single binary |
| Memcached | BSD-3-Clause | Multi-threaded, striped item locks | Slab allocator, segmented LRU | None (extstore is capacity, not durability) | None (client-side sharding) | Single binary |
| Garnet | MIT | Scales across cores on Tsavorite stores | Hybrid log, larger-than-memory | Checkpoint + AOF | Cluster + replication | .NET runtime |

IronCache's thesis sits in the empty cell: a single static binary that keeps the
Redis contract, executes across every core with a shared-nothing core, is frugal
with memory, and grows from one node to many.

## Redis OSS: the contract and the ceiling

Redis (GA line read at 8.8.0 [redis-latest-ga-version]) is the contract IronCache
must keep, and the architecture IronCache must beat. Its defining limit is that
command execution and keyspace mutation are single-threaded even when threaded
I/O is enabled: `io-threads` defaults to 1 [redis-io-threads-default], and even
when raised the I/O threads only read, parse, and write sockets while the main
thread executes every command [redis-command-execution-single-threaded]. The
new threaded-I/O model is worth real throughput (up to about 112 percent on a
multi-core Intel CPU at `io-threads=8` [redis-io-threads-throughput]), but the
keyspace is still owned by one thread. That is the gap IronCache's shared-nothing
core targets.

Redis is not a cache out of the box: `maxmemory-policy` defaults to `noeviction`
[redis-maxmemory-policy-default] [redis-default-maxmemory-policy-noeviction], and
when eviction is enabled it is not true LRU or LFU but an approximation that
samples `maxmemory-samples` (default 5) keys per eviction [redis-lru-lfu-sampling]
[redis-maxmemory-samples], using a 24-bit per-object metadata field [redis-lru-bits].
The LFU mode uses a probabilistic Morris counter (a 16-bit decay time plus an
8-bit counter, `lfu-log-factor` 10, `lfu-decay-time` 1) [redis-lfu-counter-encoding]
[redis-lfu-log-factor] [redis-lfu-decay-time]. `maxmemory` is enforced against
allocator logical bytes, not OS RSS [redis-maxmemory-accounting]. This is exactly
the place modern eviction research (below) improves on.

Memory layout is the other lesson. The classic object header is 16 bytes
[redis-robj-header-16-bytes-classic]; Redis 8.x repacks it and can embed the key
in the object to cut per-key overhead [redis-kvobj-header-redesign-8x]. Strings
up to 44 bytes are embedded [redis-embstr-threshold-44], collections use the
compact listpack encoding until tunable thresholds [redis-hash-max-listpack-entries-512]
[redis-set-encodings-thresholds] [redis-zset-max-listpack-entries-128]
[redis-list-max-listpack-size-neg2], and the main dictionary uses two-table
incremental rehashing [redis-dict-two-table-rehash] with pointer tagging to avoid
a `dictEntry` allocation for single-key buckets [redis-dict-bucket-pointer-tagging].
The cost Dragonfly highlights is the `dictEntry` itself (24 bytes, with a roughly
48N-byte peak during rehash) [redis-dictentry-size]. Redis performs no transparent
in-memory value compression; only the RDB file is compressed, with LZF
[redis-no-transparent-value-compression] [redis-rdbcompression-default-yes-lzf].

Persistence is fork-based and that is its weakness for an efficiency-first cache.
RDB snapshots and AOF rewrites fork a child, and copy-on-write can roughly double
resident memory under write load [redis-fork-cow-doubles-memory] [redis-fork-2x-memory]
[redis-cow-rss-doubling], a spike that Transparent Huge Pages makes worse by
turning the COW unit into 2 MB (hence the standard advice to disable THP)
[redis-thp-cow-blowup] [redis-thp-disable]. The fork itself stalls proportionally
to heap size (about 9 to 13 ms per GB, far worse on old virtualized hosts)
[redis-fork-latency-per-gb]. AOF is off by default [redis-appendonly-default-off],
its default `everysec` fsync can actually lose up to about 2 seconds on a slow
disk [redis-everysec-real-worst-case-2s], and modern Redis uses a multi-part AOF
directory with an RDB preamble [redis-multipart-aof-since-7.0]
[redis-aof-use-rdb-preamble-default]. IronCache takes Dragonfly's forkless route
instead.

Operationally, Redis ships 6 separate binaries with some as symlinks
[redis-separate-binaries-symlinks], bundles a forked jemalloc 5.3.0
[redis-bundled-jemalloc-version], and exposes about 240 to 246 core commands
[redis-core-command-count]. It has no native Prometheus endpoint
[redis-no-builtin-prometheus]. **Borrow** the contract, the listpack-style compact
encodings, and the pointer-tagging metadata discipline. **Reject** the
single-threaded core, the fork-based save, and the multi-binary layout.

## Valkey: the open evolution

Valkey is the BSD-3-Clause [valkey-license-bsd3] Linux Foundation fork of Redis,
created after Redis Ltd. relicensed the core to dual SSPLv1/RSALv2 in March 2024;
it forked Redis 7.2.4 [valkey-fork-origin]. It is the most relevant moving target:
the live line is already past 8.x (9.0 GA shipped 2025-10-21) [valkey-version-landscape-2026].
Its big efficiency bet is asynchronous I/O threading that offloads socket reads,
RESP parsing, replies, polling, and frees, while keeping execution single-threaded
[valkey-io-threads-offload-scope], delivered through a per-thread
single-producer/single-consumer ring buffer [valkey-io-thread-jobqueue] with
command prefetching [valkey-prefetch-batch-default]. The measured wins are large
(about 1.2M QPS on r7g, up from 380K, roughly 3.16x) [valkey-async-io-throughput]
[valkey-io-threads-throughput], though I/O threads are still off by default
[valkey-io-threads-default-off].

Valkey also attacks memory directly, which validates IronCache's memory tenet: it
replaced the chaining dict with a new open-addressing hashtable using
cache-line-sized 7-entry buckets [valkey-hashtable-replaces-dict]
[valkey-hashtable-bucket-layout], embedded the key's SDS in the dictionary entry
(about 8 bytes per key) [valkey-embedded-key-8b], and split the global dictionary
into 16,384 per-slot dictionaries [valkey-per-slot-dict-16b], together cutting RSS
about 20 percent versus 7.2 [valkey-memory-20pct] and fork COW about 47 percent.
Newer work adds dual-channel replication (off by default)
[valkey-dual-channel-default-off] and atomic slot migration [valkey-atomic-slot-migration].
**Borrow** the open-addressing low-overhead table and the per-slot partitioning
idea (they point the same way as Dashtable). **Reject** nothing on principle, but
note the ceiling: it is still the Redis execution core, not a shared-nothing one.

## KeyDB: multi-threading the shared keyspace

KeyDB (Snap-acquired in 2022 [keydb-snap-acquisition], BSD-3) multi-threads a
shared keyspace, with `server-threads` defaulting to 2 (4 recommended)
[keydb-server-threads-default], reporting roughly 5x Redis throughput
[keydb-5x-faster-claim]. It guards the shared hash table with a custom fast ticket
spinlock [keydb-fastlock-ticket-spinlock], betting that core hash-table access is
so fast that a spinlock is cheaper than sharding [keydb-spinlock-low-contention-claim].
That bet has a contention ceiling that the shared-nothing model does not. KeyDB
adds genuinely interesting pieces: an MVCC architecture for non-blocking SCAN/KEYS
and BGSAVE [keydb-mvcc-nonblocking], active-active multi-master replication using
an ephemeral per-process UUID [keydb-active-replica-uuid] with last-operation-wins
and an undefined identical-key conflict resolution [keydb-multimaster-lww-undefined],
and a FLASH tier on RocksDB for larger-than-memory data [keydb-flash-on-rocksdb]
(still labelled Beta) [keydb-flash-beta]. The catch: KeyDB 6.3.x tracks Redis 6
[keydb-redis-base-version], its last release was v6.3.4 in 2023 [keydb-latest-release-v6-3-4],
and its main branch has been effectively dormant since early 2024
[keydb-last-commit-dormant]. **Borrow** the MVCC-snapshot idea and study the
active-active conflict model. **Reject** the shared-keyspace-under-a-lock approach
and the unmaintained Redis 6 base.

## DragonflyDB: the architecture IronCache adopts

Dragonfly (v1.39.0 [dragonfly-latest-version]) is the closest thing to IronCache's
target and the single most influential source. Its core is shared-nothing
thread-per-core: the keyspace is sharded into N parts (N at most the thread count),
each shard owned by exactly one thread, with threads communicating only by message
passing and never taking a mutex on the hot path [dragonfly-shard-formula]. I/O and
scheduling run on the helio framework over io_uring (Linux 5.11+, epoll fallback)
[dragonfly-iouring-helio] using userspace fibers rather than kernel threads
[dragonfly-fibers-model], and the connection fiber doubles as the cross-shard
transaction coordinator [dragonfly-coordinator-fiber]. The two load-bearing data
structures are Dashtable, an extendible-hashing table (60 buckets per segment,
14 slots per bucket, 840 records per segment) [dashtable-segment-geometry] derived
from the Dash persistent-memory paper [dragonfly-dash-paper-citation], which cuts
per-item overhead to about 6 to 16 bytes versus the Redis dict's 16 to 32
[dashtable-overhead-bytes] (1 GB versus 1.73 GB for 20M small items
[dashtable-populate-memory]); and forkless versioned snapshotting, where a
per-shard monotonic version cut plus an on-write hook serializes a consistent
point-in-time image with constant extra memory and no fork
[dragonfly-forkless-versioned-snapshot] [dragonfly-forkless-snapshot-mechanism]
[dragonfly-snapshot-constant-memory]. Multi-key atomicity uses VLL
[dragonfly-vll-citation], and the allocator is mimalloc 2.2.4 [dragonfly-mimalloc-version].

The crucial honesty about Dragonfly's numbers shapes IronCache's benchmark
posture. The headline is "25X more throughput and up to 80 percent less
resources" [dragonfly-25x-throughput-claim], and it reaches roughly 3.8M QPS on a
c6gn.16xlarge [dragonfly-peak-qps-c6gn] and 6.43M ops/sec on a 64-vCPU Graviton3
[dragonfly-643m-rps-graviton3]. But that 25x compares Dragonfly on 64 threads to
Redis on 2 [dragonfly-25x-thread-asymmetry], and on a single core Dragonfly is
roughly at parity with Redis (SET 173K versus 159K) [dragonfly-single-core-parity].
The efficiency win is vertical scaling plus metadata reduction plus forkless
persistence, not raw single-core speed. Its snapshot is about 30 percent more
memory-efficient idle with no visible bgsave spike, while Redis peaked near 3x
[dragonfly-bgsave-memory-efficiency]. It speaks about 185 Redis commands plus
Memcached on one port [dragonfly-protocol-surface] and has a built-in
`/metrics` endpoint on its main port [dragonfly-native-prometheus-6379-metrics],
shipping as one binary [dragonfly-single-binary-gflags]. Its `cache_mode`
eviction claims a higher hit rate than LRU/LFU with zero memory overhead but is
unquantified [dragonfly-cache-mode-eviction]. **Borrow** the shared-nothing core,
io_uring, Dashtable, and forkless snapshot. **Adapt** the fiber model into Rust
async tasks and re-tune the Dashtable geometry. The strategic lesson: IronCache's
"max throughput per core" is a harder and more honest target than Dragonfly's
vertical-scaling story, and must be benchmarked single-core.

## Memcached: slab discipline and segmented LRU

Memcached (1.6.42 [latest-version-1642]) is the multi-threaded baseline (4 worker
threads by default [default-threads]) whose memory and eviction machinery are
worth studying even though it does not speak the Redis contract. Its slab allocator
carves memory into 1 MB pages cut into size classes that grow by a 1.25 factor
[slab-growth-factor] [item-size-max-1mb], trading internal fragmentation for O(1)
allocation and zero external fragmentation. Its segmented LRU (HOT, WARM, COLD,
TEMP with tuned percentages) [segmented-lru-defaults] is scan-resistant and runs
background maintainer and crawler threads by default [lru-threads-on-by-default].
extstore extends capacity to flash while keeping keys, the hash table, and small
items in RAM behind a 12-byte pointer [memcached-extstore-keys-in-ram-12b-pointer]
[memcached-extstore-defaults], and a warm-restart mode mmaps item memory so a clean
restart skips the cold-cache penalty [memcached-warm-restart-mmap-sigusr1]. The
binary protocol is deprecated in favor of the meta protocol [binary-protocol-deprecated].
**Adapt** the slab discipline (with finer or adaptive classes to beat its
fragmentation) and the warm-restart idea (made first-class and durable). **Borrow**
the striped-lock and per-thread-loop throughput lesson. **Reject** the no-contract,
no-persistence, no-clustering stance.

## Garnet: a great store behind a managed runtime

Garnet (stable v1.1.10 [garnet-latest-stable-version], default port 6379
[garnet-default-port]) proves a RESP-compatible cache can scale across cores on a
log-structured store. It uses two Tsavorite stores (a main store for strings and an
object store) [garnet-two-stores] behind a narrow four-primitive storage API (Read,
Upsert, Delete, atomic Read-Modify-Write) [garnet-narrow-waist-api], with a hybrid
log whose mutable hot region defaults to 90 percent [garnet-default-mutable-percent]
of a 16 GB log [garnet-default-log-memory-size], optional disk tiering (off by
default) [garnet-storage-tier-default-off], and an AOF that is also off by default
[garnet-aof-default-off]. Its non-TLS network path runs RESP parsing and the storage
op inline on the I/O completion for low latency [garnet-inline-io-thread-processing].
Microsoft reports higher throughput and lower, more stable tail latency than Redis,
KeyDB, and Dragonfly, though without published absolute numbers
[garnet-bench-qualitative], measured on 2x Azure F72s v2 against Redis 7.2, KeyDB
6.3.4, and Dragonfly [garnet-bench-hardware] [garnet-bench-baselines]. **Borrow**
the narrow storage API and the hybrid-log idea. **Reject** the .NET runtime: it is
why Garnet fails the single-static-binary tenet, and it is IronCache's opening.

## Eviction and admission: the heart of efficiency

This is where "most efficient" is won or lost, and where the literature has moved
far past approximated LRU. ARC self-tunes between recency and frequency using a
directory of about twice the cache size, with no frequency counters
[arc-self-tuning-no-counts]. W-TinyLFU (the Caffeine policy) couples a small LRU
admission window (about 1 percent) with a TinyLFU filter backed by a 4-bit
Count-Min Sketch costing about 8 bytes per entry, with periodic aging that halves
all counters [wtinylfu-cmsketch-4bit] [wtinylfu-window-main-split]
[wtinylfu-caffeine-sketch]. The newest and most IronCache-relevant results are the
FIFO-based policies. S3-FIFO partitions the cache into a small 10 percent
probationary FIFO and a 90 percent main FIFO with a ghost list, using a 2-bit
frequency counter capped at 3 [s3fifo-small-main-split] [s3fifo-freq-counter-2bit-cap3];
across 6594 traces it had the best miss ratio on 10 of 14 datasets
[s3fifo-miss-ratio-wins] (exploiting that a median 72 percent of objects are
one-hit wonders [s3fifo-onehit-wonder-72pct]) at roughly 6x the throughput of an
optimized LRU at 16 threads [s3fifo-throughput-6x]. SIEVE is even simpler: one FIFO
plus a moving hand and a single visited bit per object [sieve-algorithm], it was
the NSDI 2024 best paper [sieve-simpler-than-lru-nsdi24], has the lowest miss ratio
on over 45 percent of traces [sieve-miss-ratio-45pct], runs about 16 percent faster
than optimized LRU single-threaded and over 2x at 16 threads [sieve-throughput],
and took only 12 to 21 lines to retrofit into five cache libraries (with the caveat
that it has no stack property and degrades on tiny caches and scan-heavy workloads)
[sieve-loc-and-stack-property].

A subtle but decisive result for a high-throughput cache: with LRU and SLRU the
per-hit list operation is a contended bottleneck, so throughput rises and then
drops at high hit ratio, whereas FIFO-based policies see throughput rise
monotonically with hit ratio [hit-ratio-can-hurt-throughput]. That, plus the lock
friendliness of FIFO policies, is why IronCache leans toward the S3-FIFO and SIEVE
family rather than LRU. **Borrow** S3-FIFO/SIEVE as the default-policy candidates,
benchmarked by hit ratio per byte. **Adapt** W-TinyLFU's frequency sketch as an
admission filter. **Reject** approximated-LRU-as-default.

## AI and ML for caching: promising, off the hot path

The learned-caching literature is real but must stay off the critical path. LRB
(Learning Relaxed Belady) uses a gradient-boosted tree to approximate the optimal
Belady decision and cut WAN traffic 4 to 25 percent versus a production CDN cache
[lrb-model-and-traffic-reduction]. LeCaR uses online regret minimization to weight
recency versus frequency and beats ARC by more than 18x when the cache is small
relative to the working set [lecar-regret-minimization-smallcache]. Parrot trains
an attention-based policy by imitating Belady, cutting CPU-cache miss rate about 20
percent on SPEC and raising hit rate about 61 percent over LRU on a web-search
benchmark [parrot-imitation-belady-icml20]. The lesson for IronCache: inference
latency, determinism, and per-entry model memory rule ML out of the GET/SET path,
but a background advisor that retunes policy knobs and admission thresholds is
viable. Dragonfly's opaque `cache_mode` heuristic [dragonfly-cache-mode-eviction]
shows both the appeal and the need to quantify any such claim honestly.

## Concurrency and runtime: building the shared-nothing core in Rust

The shared-nothing thread-per-core model comes from Seastar/ScyllaDB (one thread
per core, explicit message passing, no shared memory) [seastar-shared-nothing], and
its central promise is that locks become unnecessary once a single thread of
execution owns each core's data, at the cost of requiring sharding
[glommio-locks-never-necessary]. Rust offers three runtime shapes. Tokio is a
multi-threaded work-stealing scheduler on a readiness (epoll/kqueue) model with two
syscalls per I/O [tokio-workstealing-readiness-model] (1.52.3, MSRV 1.71
[tokio-version-msrv]). Glommio (0.9.0, MSRV 1.70 [glommio-version-msrv]) and monoio
(0.2.4 [monoio-version]) are thread-per-core runtimes on io_uring; ByteDance reports
monoio at roughly 2x tokio at 4 cores and 3x at 16, but worse at 1 core or few
connections [monoio-vs-tokio-scaling], with an epoll/kqueue fallback below kernel
5.6 [monoio-min-kernel-fallback]. The low-level io-uring crate is at 0.7.12
[io-uring-crate-version]; key opcodes need kernel 5.6+ [io-uring-read-opcode-kernel]
and multishot recv needs 6.0+ [io-uring-multishot-recv-kernel], while tokio-uring
wants 5.11+ [tokio-uring-min-kernel]. If a shared concurrent map were used instead
of strict sharding, the candidates are dashmap (sharded RwLocks, shard count
`next_power_of_two(parallelism * 4)`) [dashmap-internal-design] [dashmap-default-shards],
scc (3.7.3 [scc-version]), and the lock-free papaya (Swiss-table with hyaline
reclamation) [papaya-version-reclamation]. **Borrow** thread-per-core on io_uring
with a portable fallback, enforced by Rust ownership so one core owns one shard.
**Adapt** the runtime choice to keep a non-uring path for macOS and old kernels.

## Memory: allocators, fragmentation, layout

Redis bundles jemalloc 5.3.0 [redis-bundled-jemalloc-version]; upstream jemalloc
defaults `narenas` to 4x CPUs [jemalloc-narenas-default], disables the background
purge thread [jemalloc-background-thread-default] (Redis re-enables it), and decays
dirty pages over 10 seconds [jemalloc-decay-defaults]. Dragonfly chose mimalloc,
reported about 13 percent faster than jemalloc on one benchmark and over 2.5x on
another (2021 hardware) [mimalloc-benchmarks], available to Rust as the mimalloc
crate 0.1.52 [mimalloc-rust-version] or jemalloc via tikv-jemallocator 0.7.0
[tikv-jemallocator-version]. Fragmentation is the operational tax: Redis defines
`mem_fragmentation_ratio` as RSS over used memory [redis-fragmentation-ratio], and
THP must be off to avoid both COW blowup and fragmentation [redis-thp-disable].
**Borrow** a sharded, low-fragmentation allocator and per-shard arenas; the choice
interacts with returning freed pages to the OS so the memory claim holds in RSS,
not just logical bytes.

## Compression: the unclaimed memory win

Redis leaves transparent value compression on the table (none in memory; LZF only
in RDB) [redis-no-transparent-value-compression] [redis-rdbcompression-default-yes-lzf],
and clients that do compress (for example spymemcached, threshold 16 KB)
[spymemcached-default-compression-threshold] prove the demand. The codec trade
space is well measured on Silesia: LZ4 at ratio 2.10 and about 780/4970 MB/s
compress/decompress [lz4-silesia-benchmark], Snappy and LZF a touch behind
[snappy-lzf-silesia-benchmark], and Zstd at ratio 2.90 and about 510/1550 MB/s at
level 1 [zstd-silesia-benchmark-l1] with fast negative levels for more speed
[zstd-fast-modes-benchmark]. The standout for a cache full of small similar values
is dictionary compression: Zstd dictionaries took 1000 small records from 2.8x to
6.9x [zstd-dictionary-small-data-6.9x], with a default trained dictionary around
110 KB [zstd-dictionary-default-size-110kb]. Pure-Rust options exist (lz4_flex at
about 1272/4540 MiB/s safe mode [lz4-flex-safe-vs-c]; the zstd crate 0.13.3 binding
zstd 1.5.7 [zstd-rust-crate-version]). **Borrow** an opt-in, size-and-entropy-gated
transparent compression tier with trained dictionaries for small values, a feature
Redis simply does not have.

## Persistence and storage engines: beyond fork

The FASTER lineage behind Garnet is the model for larger-than-memory and durable
state without a fork. FASTER's HybridLog is one address space split into mutable,
read-only, and on-disk regions [faster-hybridlog-three-regions], made lock-free by
epoch protection [faster-epoch-protection], with a cache-line bucket index (7 hash
entries plus an overflow pointer, 15-bit tags) [faster-hash-bucket-layout], hitting
about 160M ops/sec in memory [faster-throughput-160m]. Its successor F2 is an
explicit hot-log/cold-log two-tier store [f2-hot-cold-log-two-tier] reporting 2 to
11.9x over existing stores (about 11.8x over RocksDB) [f2-throughput-vs-rocksdb].
The alternative cold backend, an LSM like RocksDB (KeyDB FLASH [keydb-flash-on-rocksdb]),
carries write amplification often over 10x [rocksdb-leveled-write-amp-over-10],
which is why a hybrid log or the Memcached extstore layout (RAM index, flash values)
[memcached-extstore-keys-in-ram-12b-pointer] is more attractive for a cache.
**Borrow** the hybrid-log idea and the forkless snapshot; **reject** a write-amplifying
LSM on the hot path.

## Distribution: from one node to many

Redis Cluster is the contract reference: a fixed 16384 hash slots
[redis-cluster-hash-slots-16384] keyed by CRC16/XMODEM [redis-cluster-crc16-xmodem-rrc],
a gossip bus at client port plus 10000 [redis-cluster-bus-port-offset], client-side
MOVED/ASK redirection [redis-cluster-moved-ask], hash tags for multi-key locality
[redis-cluster-hash-tag-rule], a 15000 ms node timeout [redis-cluster-node-timeout-default-rrc],
and full-coverage-required-by-default [redis-cluster-require-full-coverage-default];
16384 (not 65536) was chosen to keep the per-node slot bitmap small
[redis-cluster-why-16384]. Replication is asynchronous [redis-cluster-async-replication],
and WAIT confirms in-memory replica receipt, not durability or strong consistency
[redis-wait-since-and-caveat] [redis-wait-not-strongly-consistent], with WAITAOF
adding local fsync confirmation [redis-waitaof-since-and-semantics] and PSYNC2 a
secondary replication id for partial resync after failover [redis-psync2-secondary-replid].
Sentinel HA needs a majority to elect, not just the quorum to detect
[redis-sentinel-quorum-vs-majority], and the Redis-Raft module's Jepsen run found 21
issues including split-brain and lost updates [jepsen-redis-raft-21-issues], a
warning about how hard strong consistency is. The alternative partitioning schemes
are jump consistent hash (O(ln n), zero memory, but sequential buckets only)
[jump-hash-constant] [jump-hash-limitation], SWIM for O(1) membership
[swim-scalability], Raft for the slot map [raft-overview], and Dynamo-style sloppy
quorums with hinted handoff for leaderless availability [dynamo-quorum-sloppy-hinted].
Active-active needs CRDTs: Redis Enterprise maps strings to last-write-wins
registers, counters to PN-counters, and sets to add-wins OR-Sets
[redis-crdb-datatype-mapping], while KeyDB's simpler last-write-wins leaves
identical-key conflicts undefined [keydb-multimaster-lww-undefined]. **Borrow** the
slot model and smart-client redirection for contract compatibility. **Adapt** the
membership and consensus to a single-node-first design that grows out cleanly.

## The RESP contract: what "Redis-compatible" means

RESP3 is the target. Each type has a one-byte prefix (15 markers including map,
set, push, double, big number, verbatim) [resp-type-prefixes], a connection starts
in RESP2 and upgrades only via `HELLO 3` [resp3-opt-in-via-hello] (returning
`-NOPROTO` on an unsupported version [hello-noproto-error]), with different null
encodings between the versions [resp2-null-encodings] and a 512 MB bulk-string cap
[bulk-string-max-512mb]. The contract has sharp edges to preserve: transactions
queue at MULTI and have no rollback on a runtime error [multi-exec-no-rollback];
sharded pub/sub arrived in 7.0 [sharded-pubsub-7.0]; client-side caching uses
CLIENT TRACKING with BCAST/OPTIN/OPTOUT/NOLOOP and a RESP2 invalidation channel
[client-tracking-options] [resp2-invalidation-channel]; keyspace notifications are
off by default [keyspace-notifications-off-by-default]; ACL ships a permissive
default user [acl-default-user]; and modern official clients already default to
RESP3 even though the server defaults to RESP2 [client-default-resp3-redis8].
Compatibility is tiered against the roughly 240-command surface
[redis-core-command-count]. **Borrow** RESP2 and RESP3 and the observable semantics
of the supported commands, documenting the unsupported surface explicitly.

## Operability and the single binary

The single-binary, self-updating story is achievable in Rust. The musl targets
produce a static binary (crt-static is on by default for x86_64-unknown-linux-musl,
though not every musl target) [rust-musl-crt-static-default], cross-built with
cargo-zigbuild [cargo-zigbuild-version-features] and made reproducible with an
embedded dependency manifest via cargo-auditable [cargo-auditable-version-reproducible].
Self-update is a solved problem (the self_update crate fetches and atomically
replaces, though without rollback [self-update-crate-version-backends]; self-replace
does the atomic swap [self-replace-atomic-rename]), and the rollback gap is exactly
what IronCache's `upgrade` should close. Dragonfly's single self-contained binary
[dragonfly-single-binary-gflags] is the model versus Redis's 6 binaries
[redis-separate-binaries-symlinks]. Observability should be native, not bolted on:
Redis has no built-in Prometheus endpoint [redis-no-builtin-prometheus] while
Dragonfly serves `/metrics` on its main port [dragonfly-native-prometheus-6379-metrics],
and the INFO sections [redis-info-sections], SLOWLOG (10 ms threshold, 128 entries)
[redis-slowlog-defaults], and latency monitor (off by default)
[redis-latency-monitor-default-off] plus CONFIG SET/REWRITE without a SIGHUP reload
[redis-config-set-rewrite-no-sighup] define the operability contract to match.
Packaging matters too: Valkey installs as a plain Homebrew formula while Redis now
needs a tap [valkey-brew-plain-formula]. **Borrow** the single binary, native
metrics, and the INFO/SLOWLOG surface. **Adapt** self-update to add rollback.

## Benchmarking and correctness: proving the claim honestly

The benchmark-war traps are well known and IronCache must avoid them. memtier's
defaults (pipeline 1 [memtier-default-pipeline-1], 1:10 SET:GET
[memtier-default-ratio-1-10], uniform random keys [memtier-default-key-pattern-RR],
p50/p99/p99.9 [memtier-default-percentiles]) and redis-benchmark's single-key
default [redis-benchmark-single-key-default] each distort results unless corrected;
realistic runs need Zipfian keys [memtier-supports-zipfian] and the standard YCSB
workload mix [ycsb-core-workloads]. The deepest trap is coordinated omission:
closed-loop generators under-report tail latency, which must be corrected with rate
control and HdrHistogram [coordinated-omission-closed-loop]. Dragonfly's 25x is the
canonical example of an unfair thread-count comparison [dragonfly-25x-thread-asymmetry],
and Garnet published only relative numbers on stated hardware [garnet-bench-baselines].
Correctness needs more than benchmarks: deterministic simulation testing in the
FoundationDB/TigerBeetle style (single thread, simulated I/O, one replayable seed)
[dst-fdb-tigerbeetle-single-seed], Jepsen with Elle's cycle-detection checker for
any clustering [elle-cycle-detection-anomalies] (the Redis-Raft run is the cautionary
tale [jepsen-redis-raft-21-issues]), and a per-command conformance suite against
real Redis. **Borrow** memtier and YCSB with honest methodology, per-core and
tail-latency-first; **adopt** deterministic simulation and a conformance suite as
gates.

## Notable corrections from verification

The adversarial verification pass changed these load-bearing values. They are
surfaced here so a reviewer can re-verify them first; the corrected values are what
`claims.yaml` and this survey use.

- Dragonfly's memory numbers (about 30 percent idle, about 3x peak) are real but
  live in the README, not the cited blog [dragonfly-memory-snapshot-claim].
- Valkey's 1.19M RPS figure was measured on c7g.16xlarge, not 4xlarge, and the
  exact source URL was corrected [valkey-io-threads-throughput]; the fork date and
  base version were pinned precisely [valkey-fork-origin].
- F2 beats RocksDB by about 11.8x on average, not the originally claimed 3.2x
  [f2-throughput-vs-rocksdb]; the FASTER bucket layout is 7 entries plus an overflow
  pointer with 15-bit tags [faster-hash-bucket-layout].
- The scc crate is at 3.7.3, not 3.4.8 [scc-version]; the zstd crate binds via
  zstd-sys 2.0.14, not 2.0.16 [zstd-rust-crate-version].
- W-TinyLFU details were corrected: production Caffeine has no doorkeeper, and aging
  halves counters at a sample threshold [wtinylfu-cmsketch-4bit]
  [wtinylfu-caffeine-sketch]; SIEVE integrates into five named libraries
  [sieve-loc-and-stack-property].
- KeyDB's last main-branch commit is 2024-04-04 [keydb-last-commit-dormant];
  mimalloc's benchmark figures are from a 2021 run [mimalloc-benchmarks].
- Several Redis facts had correct values but wrong cited sources or line numbers,
  now fixed: single-threaded execution [redis-command-execution-single-threaded],
  the LRU clock constants [redis-lru-clock-resolution], the COW doubling and its
  headroom advice [redis-cow-rss-doubling] [redis-fork-2x-memory], and the cluster
  node timeout source [redis-cluster-node-timeout-default].
- crt-static is on by default for x86_64 musl but not all musl targets
  [rust-musl-crt-static-default].
