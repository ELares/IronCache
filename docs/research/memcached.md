# Research: Memcached (slab allocator, segmented LRU)

> Part of the IronCache prior-art research corpus (`docs/research/`). This
> document is DESCRIPTIVE: it records what other systems do, with
> version-pinned claims tracked in [`../prior-art/claims.yaml`](../prior-art/claims.yaml).
> Prescriptive IronCache decisions live in the design issues, not here.
>
> Area: `area:memory`. Claims gathered by an AI research agent from primary sources,
> then load-bearing claims independently re-checked by an adversarial verifier.

## Summary

Memcached (latest release 1.6.42, released 2026-05-18; version confirmed against git tags and the ReleaseNotes wiki) is a multi-threaded in-RAM key-value cache whose defining design is the slab allocator. Memory is carved into fixed 1MB pages (slab_page_size), each page assigned to one of up to 64 slab classes and cut into equal-size chunks. Chunk sizes start at slab-min-size 48 bytes and grow geometrically by the -f growth factor (default 1.25, then chunk-aligned). A page, once assigned to a class, is never re-cut, so the allocator trades internal fragmentation (each item rounds up to the next chunk size) for O(1) alloc/free with no external fragmentation. Default item_size_max is exactly 1MB (min 1k, max 1G); slab_chunk_size_max is page/2 = 512KB. A slab automover/rebalance thread can reassign whole pages between classes as the working set shifts.

Eviction uses a per-slab-class segmented LRU with four sub-queues: HOT, WARM, COLD, and TEMP. New items load into HOT, a non-reordered probationary queue; at its tail an item moves to WARM if it has been hit twice (ACTIVE bit set) or to COLD otherwise. WARM holds scan-resistant active items; inactive ones drain to COLD, the unbounded main LRU from whose tail items are actually evicted. TEMP (off by default) isolates very short-TTL items so they neither bump nor get evicted. HOT and WARM are size-capped by hot_lru_pct=20 and warm_lru_pct=40, with age caps hot_max_factor=0.2 and warm_max_factor=2.0 relative to COLD's tail age. The lru_maintainer thread juggles items between segments, enforces limits, and reclaims expired tail items; the separate lru_crawler walks every sub-LRU reclaiming expired items proactively using TTL histograms. lru_segmented is true by default; notably settings_init sets lru_maintainer_thread=false, but main() sets start_lru_maintainer=true and start_lru_crawler=true, so both threads run by default unless disabled via no_lru_maintainer/no_lru_crawler.

Memcached has been multi-threaded since the 1.2 era: -t defaults to 4 worker threads, each owning its own libevent event_base (created with EVENT_BASE_FLAG_NOLOCK for lock-free per-thread loops). Concurrency on the single global hash table (default HASHPOWER_DEFAULT=16, i.e. 65536 buckets, growing at runtime) is protected by a hash-striped array of item_locks; the lock-table size scales with thread count (power 10-15, i.e. 1024-32768 locks). This sharded-lock design is the core of memcached's throughput-per-core story.

Extstore (-o ext_path=/file:5G) extends capacity onto SSD/flash: keys, the hash table, and item metadata stay in RAM (a ~12-byte pointer header per item points to flash), while values larger than ext_item_size (default 512 bytes) are written to disk in pages (ext_page_size default 64M) via a write buffer (ext_wbuf_size default 8M) and ext_threads (default 1) IO workers, with background compaction. Extstore is explicitly NOT durability. Separately, the restartable/warm-cache feature (-e/--memory-file, since 1.5.18, marked EXPERIMENTAL) mmaps item memory to a file on a tmpfs/pmem mount; on a clean SIGUSR1 shutdown it writes a .meta file, and on restart it fixes up pointers and rebuilds the hash table in a few seconds. It is invalidated if -m, max item size, slab chunk sizes, CAS, or slab-reassign settings change, requires a stable clock, loses writes during downtime, and is incompatible with extstore.

Three wire protocols exist: the classic ASCII text protocol; the binary protocol (introduced 2008, officially deprecated since 1.6.0, frozen to bugfixes only); and the meta protocol (since 2019), a flag-based ASCII protocol that subsumes binary's features, is cross-compatible with text, and is recommended for all new clients. Explicit non-features: memcached offers no persistence (extstore and warm-restart are capacity/restart conveniences, not durability guarantees), no replication, and no server-side clustering — horizontal scale comes purely from client-side sharding (consistent hashing across an independent server pool).

## Mechanisms: borrow, adapt, or reject

| Mechanism | System | Stance | What it does | Rationale for IronCache |
| --- | --- | --- | --- | --- |
| Slab allocator (fixed pages + power-law chunk classes) | Memcached | **adapt** | Carves RAM into 1MB pages assigned to up to 64 size classes whose chunks grow by factor 1.25; O(1) alloc/free, no external fragmentation, internal fragmentation per item. | Adopt slab/size-class allocation for O(1) predictable memory, but reduce internal fragmentation with finer/adaptive class sizing; the plain 1.25 power-law wastes too much for IronCache's minimal-memory goal. |
| Segmented LRU (HOT/WARM/COLD/TEMP) | Memcached | **adapt** | Per-class four-segment LRU with an ACTIVE bit; scan-resistant and batches reordering to cut lock contention vs naive LRU. | Scan resistance and low-contention reordering are right, but TinyLFU/S3-FIFO/SIEVE beat segmented LRU on hit ratio per byte; borrow segment-draining/async-bump, adapt the policy. |
| Striped item locks + per-thread libevent loops | Memcached | **borrow** | Each worker thread owns a NOLOCK libevent base; the global hash table is guarded by a hash-striped lock array sized to thread count. | Sharded locks + thread-local event loops are the proven path to throughput-per-core; maps cleanly to a Rust per-core runtime and sharded maps. |
| Slab automover / page rebalancer | Memcached | **adapt** | Background thread moves whole 1MB pages between slab classes when one is starved and another over-provisioned. | Rebalancing is required once you commit to size classes, but page-granularity moves are coarse; adapt with finer migration tied to IronCache's AI-driven tuning. |
| Extstore (RAM index + flash values) | Memcached | **borrow** | Keeps keys/hash/metadata + small items in RAM, spills large values to SSD via a ~12B pointer header, write-buffered with background compaction. | Tiered RAM-index/flash-value is a strong fit for cost-efficient large caches; borrow the architecture but pair it with real durability so it isn't 'not persistence'. |
| Warm restart via mmap memory-file | Memcached | **adapt** | mmaps item memory to a tmpfs/pmem file; clean shutdown writes metadata and restart rebuilds pointers/hash in seconds. | Fast restart without a cold cache is valuable for a single-binary product; adapt into a first-class, non-experimental snapshot/restore that also supports real persistence. |
| Meta protocol (flag-based ASCII) | Memcached | **borrow** | Extensible ASCII command set with flags/tokens that subsumes the binary protocol and is text-compatible. | IronCache is Redis-wire-compatible so meta isn't the primary protocol, but its flag-extensibility (vs a rigid binary framing) is a lesson for any IronCache-native extensions. |
| No persistence / replication / clustering (client-side sharding) | Memcached | **reject** | Standalone node; scale-out is entirely client-side consistent hashing; no built-in durability or replication. | IronCache explicitly targets single-node to multi-node with smart features; rejecting the no-clustering stance is a core differentiator, though client-side sharding stays a valid baseline mode. |

## Implications for IronCache

- Memory efficiency is bounded by the size-class scheme: memcached's 1.25 factor leaves measurable internal fragmentation, so IronCache must evaluate finer/adaptive classes or compaction to actually beat it on bytes-per-item.
- The striped-lock + per-thread NOLOCK event-loop design is the validated route to throughput-per-core and maps directly onto a Rust thread-per-core runtime with sharded maps.
- Segmented LRU is a solid scan-resistant baseline but not state of the art; IronCache should benchmark TinyLFU/S3-FIFO/SIEVE against it and likely ship a more memory-efficient policy.
- Extstore proves the RAM-index/flash-value tier works; IronCache can borrow it but must add genuine durability (which memcached deliberately omits) to differentiate.
- The 1MB default item-size cap is a memcached convention, not a law; a Redis-compatible IronCache must handle large values and should treat large-item handling as a deliberate design choice.
- Warm-restart-via-mmap shows fast restart is feasible; making it non-experimental and clock-independent is a concrete IronCache improvement.
- Meta protocol's flag extensibility is a model for evolving a wire protocol without a binary rewrite, useful for IronCache extensions beyond the Redis wire format.
- memcached defaults -t to 4 regardless of cores; IronCache should auto-scale worker count to detected cores for strong out-of-box throughput.

## Key claims

Load-bearing claims are marked. The `id` cross-references `claims.yaml`.

| id | system | version | value | conf. | check |
| --- | --- | --- | --- | --- | --- |
| `binary-protocol-deprecated` * | Memcached | 1.6.42 | binary deprecated since 1.6.0 (intro 2008); meta since 2019, recommended for new clients | high | self-verified |
| `default-threads` * | Memcached | 1.6.42 | 4 | high | self-verified |
| `extstore-defaults` * | Memcached | 1.6.42 | ext_item_size=512B, ext_page_size=64M, ext_wbuf_size=8M, ext_threads=1 | medium | self-verified |
| `hashpower-default` | Memcached | 1.6.42 | HASHPOWER_DEFAULT=16 (65536 buckets) | high | self-verified |
| `item-size-max-1mb` * | Memcached | 1.6.42 | 1MB default (min 1k, max 1G); slab_page_size=1MB | high | self-verified |
| `latest-version-1642` * | Memcached | 1.6.42 | 1.6.42, released 2026-05-18 | high | self-verified |
| `lru-threads-on-by-default` * | Memcached | 1.6.42 | start_lru_maintainer=true, start_lru_crawler=true in main() | high | self-verified |
| `max-slab-classes` | Memcached | 1.6.42 | 64 (63+1) | high | self-verified |
| `segmented-lru-defaults` * | Memcached | 1.6.42 | hot_lru_pct=20, warm_lru_pct=40, hot_max_factor=0.2, warm_max_factor=2.0; lru_segmented=true | high | self-verified |
| `slab-chunk-max` | Memcached | 1.6.42 | 512KB (slab_page_size/2) | high | self-verified |
| `slab-growth-factor` * | Memcached | 1.6.42 | 1.25 | high | self-verified |
| `slab-min-chunk` | Memcached | 1.6.42 | 48 bytes | high | self-verified |
| `warm-restart` * | Memcached | 1.6.42 | -e/--memory-file (EXPERIMENTAL, since 1.5.18); stop with SIGUSR1; incompatible with extstore | high | self-verified |

`*` = load-bearing. `check`: result of the independent adversarial re-verification.

## Research papers and primary sources

- **An Empirical Analysis on Memcached's Replacement Policies** (MEMSYS 2023). Dissects memcached's segmented (HOT/WARM/COLD) LRU and compares hit ratio/overhead vs simple LRU and alternatives on real traces. [source](https://dl.acm.org/doi/fullHtml/10.1145/3631882.3631883) Relevance: Directly informs whether IronCache keeps, tunes, or replaces segmented LRU; quantifies its strengths/weaknesses.
- **TinyLFU: A Highly Efficient Cache Admission Policy** (ACM TOS 2017 (Einziger et al.)). A frequency-sketch admission filter caches an item only if it would beat the eviction victim, giving near-optimal hit ratios at tiny metadata cost. [source](https://dl.acm.org/doi/10.1145/3149371) Relevance: Candidate replacement/augmentation for segmented LRU under IronCache's minimal-memory, max-hit-ratio goal.
- **SIEVE is Simpler than LRU: an Efficient Turn-Key Eviction Algorithm** (NSDI 2024 (Zhang et al.)). A single-bit, lazy-promotion eviction algorithm that beats LRU on hit ratio and is much cheaper to make concurrent. [source](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo) Relevance: Low-lock, high-hit-ratio eviction matches IronCache's throughput-per-core + memory targets; strong alternative to segmented LRU.
- **Workload Analysis of a Large-Scale Key-Value Store (Facebook Memcached)** (SIGMETRICS 2012 (Atikoglu et al.)). Characterizes real memcached traffic: tiny values dominate, heavy read skew, diurnal patterns, and resulting fragmentation behavior. [source](https://dl.acm.org/doi/10.1145/2254756.2254766) Relevance: Grounds IronCache's size-class, fragmentation, and compression assumptions in real cache workloads.

## Open questions

- What are extstore's exact default constants in the 1.6.42 source (storage.c/extstore.h) versus the docs values (ext_item_size=512, ext_page_size=64M, ext_wbuf_size=8M, ext_threads=1)? Docs were used; source not yet cross-checked.
- How much internal fragmentation does the 1.25 factor cost on realistic value distributions, and can a Rust allocator with adaptive classes beat it meaningfully?
- Does memcached's slab automover converge under shifting working sets or thrash, and what thresholds drive it?
- How does segmented LRU's hit ratio compare to TinyLFU/SIEVE/S3-FIFO at equal memory on standard cache traces?
- What is real-world warm-restart recovery time vs dataset size, and how brittle are the clock/settings invalidation conditions?
- Given Redis wire compat, is there value in also supporting a meta-style extensible protocol, or does it dilute focus?

## Proposed issues (seeds for the tracker)

- **[decision, M1]** Decide slab/size-class allocator strategy and fragmentation budget. Choose memcached-style power-law classes, adaptive classes, or a jemalloc-style allocator; set an explicit internal-fragmentation budget that beats the 1.25-factor baseline.
- **[research, M1]** Benchmark eviction policy: segmented LRU vs TinyLFU/SIEVE/S3-FIFO. Run standard cache traces comparing hit ratio and per-item metadata cost; pick IronCache's default eviction policy and document the rationale.
- **[design, M1]** Design striped-lock + per-core runtime concurrency model. Adapt memcached's per-thread NOLOCK event loops and hash-striped item locks to a Rust thread-per-core architecture; define lock-table sizing relative to core count.
- **[design, M2]** Design RAM-index/flash-value tier (extstore-equivalent) with durability. Borrow extstore's RAM-keys/flash-values layout but add real persistence so it is not merely capacity extension; specify spill threshold, write buffer, compaction.
- **[non-goal, M0]** Non-goal for M0/M1: server-side clustering and replication. Like memcached, ship single-node first with client-side sharding as baseline; defer built-in clustering/replication to a later milestone as an explicit differentiator.
- **[design, M2]** Spec fast warm-restart / snapshot-restore as first-class. Adapt memcached's mmap memory-file warm restart into a non-experimental, clock-independent snapshot/restore with pointer fix-up on boot.
- **[task, M0]** Verify memcached extstore default constants against 1.6.42 source. Read storage.c/extstore.h at tag 1.6.42 to confirm ext_item_size/ext_page_size/ext_wbuf_size/ext_threads defaults currently sourced only from docs.
