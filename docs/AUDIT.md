# Pre-implementation audit (2026-06-13)

Before any engine code is written, every IronCache issue (the vision EPIC
#1 and the 99 design/research/decision/non-goal issues) was audited for
correctness and completeness, the risky prior-art claims were re-verified
against primary sources, and the issue tree was scanned for coverage gaps.
The detailed, per-issue findings live as a comment on each issue; this file
is the consolidated record.

## Method

A fan-out of one auditor per issue assigned a verdict (valid / needs-more-info
/ has-a-bug / too-large / duplicate) with evidence; every flagged issue was
then re-checked by an independent adversarial reviewer (refute-by-default).
In parallel, 80 risky claims (previously corrected, self-verified only, or
non-high-confidence) were re-verified against fresh primary sources, and eight
coverage lenses plus a duplicate detector and a synthesis critic scanned the
whole tree. A deterministic check first validated every `[id]` and `#N`
cross-reference.

## Results at a glance

| Verdict | Count |
| --- | --- |
| valid | 55 |
| has-a-bug | 38 |
| too-large | 6 |
| needs-more-info | 1 |

- Every non-valid verdict was upheld by the independent confirmation pass.
- Claim re-verification (80 claims): 72 confirmed, 8 corrected.
- 5 claims corrected in `prior-art/claims.yaml` (provenance preserved, `verification.reaudited: 2026-06-13`).
- 36 coverage-gap issues filed (#128 to #163), 27 split sub-issues filed from 9 decomposed parents.
- 7 duplicate/overlap clusters recorded for the maintainers (see below).

## Claims corrected

- **`dragonfly-643m-rps-graviton3`** (corrected): 6.43M ops/sec on c7gn.16xlarge (64 vCPU, 128GiB); P50 0.3ms, P99 1.1ms, P99.9 1.5ms; 256B values; 10M keys; memtier_benchmark -t 64 -c 40 -n 200000 with GET-only ratio (1:0)
- **`extstore-defaults`** (corrected): ext_item_size=512B, ext_page_size=64M, ext_wbuf_size=4M, ext_threads=1
- **`valkey-io-threads-throughput`** (corrected): Primary source confirms ~1.19M RPS (Valkey 8.0) vs ~360K (7.2) = ~230% increase, measured on an AWS EC2 C7g.16xlarge (NOT c7g.4xlarge), tested with 8 I/O threads, 3M keys, 512-byte values; average latency fell 69.8% from 1.792ms to 0.542ms.
- **`valkey-brew-plain-formula`** (corrected): valkey IS a plain Homebrew core formula (`brew install valkey`, stable 9.1.0, BSD-3-Clause). But the claim that 'redis now requires brew tap redis/redis + brew install --cask redis' is OUTDATED/REFUTED as of 2026-06-13: redis is ALSO a plai
- **`cargo-dist-installer-curl-sh`** (corrected): Latest is dist v0.32.0 (2026-05-22), not ~0.31.0/2025 (project rebranded cargo-dist -> 'dist', repo still axodotdev/cargo-dist). Shell installer: curl --proto '=https' --tlsv1.2 -LsSf <url> | sh; embeds SHA256 checksums and validates tarbal
- **`io-uring-sqpoll-registered-buffers`** (corrected): Mechanism confirmed: with IORING_SETUP_SQPOLL a kernel poll thread lets the app submit/reap I/O without io_uring_enter() on the hot path (man page: 'submit and reap I/Os without doing a single system call'). The throughput figures ~238k tx/
- **`mimalloc-benchmarks`** (corrected): The ~13% speedup on the leanN/Lean benchmark is vs TCMALLOC, not jemalloc. Exact readme text: 'there is a 13% speedup over _tcmalloc_'. The sh6bench figure IS vs jemalloc: 'more than 2.5x faster than _jemalloc_'. Hardware/context confirmed:
- **`zstd-rust-crate-version`** (corrected): zstd 0.13.3 -> zstd-safe 7.1.0 -> zstd-sys 2.0.16+zstd.1.5.7 (bundles zstd C 1.5.7). The recorded value's '2.0.14 NOT 2.0.16' is wrong; 2.0.16 is correct. Features bindgen, pkg-config, zstdmt, experimental, zdict_builder, no_asm, legacy, th

## Issues decomposed (too-large) into filed sub-issues

- **#11** [NON-GOAL]: fork()+copy-on-write snapshotting, mandatory pro -> #101, #102, #103
- **#22** [DESIGN]: Security surface (AUTH, requirepass, ACL, embedded -> #104, #105, #106
- **#29** [DESIGN]: Cross-shard coordinator and transaction/scripting  -> #107, #108, #109
- **#35** [DESIGN]: Hash table, data-structure encodings, and per-key  -> #110, #111, #112, #113
- **#39** [TASK]: intset and HyperLogLog sparse/dense encodings for wi -> #114, #115, #116
- **#41** [DECISION]: Default global allocator and memory-accounting s -> #117, #118
- **#82** [DECISION]: clap subcommands vs argv[0] symlink mode-switchi -> #119, #120
- **#84** [TASK]: Packaging, cross-build matrix, reproducible builds,  -> #121, #122, #123, #124, #125
- **#88** [DESIGN]: AI-driven background advisor (expert selection + b -> #126, #127

## Coverage-gap issues filed

### M0: Vision and Scope
- #132 [NON-GOAL]: Redis Streams (XADD/XREAD/XRANGE, consumer groups, rax index) out of scope for
- #146 [DECISION]: Headline scale-out targets (max nodes, slots-per-node working range, rebalance
- #155 [DECISION]: Advisor default posture (ship shadow/off by default; engine fully correct and 
- #156 [NON-GOAL]: The advisor adds no runtime network, model-service, or GPU dependency; AI is b
- #157 [DECISION]: Per-tenet acceptance targets and release gates for Compatible/Efficient/Simple
- #162 [RESEARCH]: Pin the second-tier KV/cache landscape (Aerospike, Tarantool, Kvrocks, Hazelca

### M1: Architecture Specification
- #128 [DESIGN]: Per-data-type command semantics for strings, lists, hashes, sets, and sorted set
- #129 [DESIGN]: Generic keyspace commands and SCAN cursor-stability contract (SCAN/HSCAN/SSCAN/Z
- #133 [DECISION]: Geo command family (GEOADD/GEOSEARCH/GEODIST) scope vs non-goal
- #136 [RESEARCH]: Large-collection structure bake-off (zset ordered index: skiplist vs B-tree/AR
- #137 [DESIGN]: Connection admission, max-clients, output-buffer limits, and the OOM-write/DoS c
- #138 [DESIGN]: RESP request-size and adversarial-input hardening (proto-max-bulk-len tunable, m
- #140 [DESIGN]: Idle-connection timeout, TCP keepalive, and dead-peer reaping
- #141 [DECISION]: Per-command operational latency budget and slow-operation guard under defrag/e
- #142 [DESIGN]: Written threat model (assets, trust boundaries, attacker capabilities, STRIDE pe
- #144 [TASK]: Continuous dependency-vulnerability and license auditing as a merge/release gate (
- #145 [DESIGN]: Secrets handling: log/MONITOR redaction, in-memory zeroization, and core-dump/sw
- #147 [DESIGN]: Replica-read contract (READONLY/READWRITE, replica routing, bounded staleness su
- #149 [DESIGN]: Cluster bootstrap and node-lifecycle (seed/MEET join, learner-to-voter-to-slot-o
- #150 [DESIGN]: Admin/introspection command family (CLIENT LIST/INFO/KILL/PAUSE/NO-EVICT/NO-TOUC
- #152 [DESIGN]: Metric/label registry, native INFO field catalog, and per-command cardinality bo
- #153 [DESIGN]: AI advisor observability, explainability, and decision/audit trail (knob from->t
- #154 [DESIGN]: Advisor evaluation and promotion gate (offline replay + shadow A/B proving a cha
- #159 [DESIGN]: Continuous performance-regression CI gate (per-commit throughput-per-core and by

### M2: Prototype-Ready Design
- #130 [DESIGN]: Blocking command semantics (BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZMPOP, WAIT, XRE
- #131 [DESIGN]: Bitmap and BITFIELD semantics over the string type (SETBIT/GETBIT/BITCOUNT/BITPO
- #134 [DESIGN]: Sorted-set (zset) large representation: ordered index plus parallel member->scor
- #135 [DESIGN]: List (quicklist-equivalent) representation: linked listpack chunks with O(1) hea
- #139 [DESIGN]: Graceful shutdown contract (SHUTDOWN [NOSAVE|SAVE], SIGTERM/SIGINT, connection d
- #143 [DESIGN]: At-rest encryption of snapshots, warm-restart state, and tiered-store SSD files
- #148 [DESIGN]: Rebalancing policy and orchestration (when/which partitions move, hot-slot trigg
- #151 [DESIGN]: Troubleshooting introspection commands (MEMORY USAGE/STATS/DOCTOR, LATENCY DOCTO
- #158 [DESIGN]: Real client-driver compatibility matrix (run lettuce/redis-py/go-redis/ioredis/n
- #160 [TASK]: Determinism replay-contract verification as a CI gate (same seed yields byte-ident
- #161 [TASK]: Long-horizon soak and memory-stability correctness gate (no leak, bounded fragment
- #163 [RESEARCH]: Foundational CRDT literature and OR-Set tombstone GC for the full Redis type s

## Duplicate / overlap clusters (for maintainer review)

These existing-issue overlaps were flagged by the duplicate detector. They are
recorded, not auto-merged (merging is a maintainer decision tracked on #5).

- #19, #23, #29, #30: These four issues collectively cover the transaction-and-scripting surface and overlap heavily. #30 is the DECISION on transaction/scripting-surface s _(make-dependency: keep #30 as the single DECISION t)_
- #11, #44: #11 is a NON-GOAL declaring fork()+copy-on-write snapshotting, a mandatory proxy, and host THP/overcommit tuning out of scope. #44 is a DECISION on 'T _(keep-separate-but-cross-link: a NON-GOAL register )_
- #7, #8, #89: #7 is the DECISION that headline metrics are throughput-per-core and memory-at-fixed-hit-ratio. #89 is RESEARCH to 'Define the advisor objective metri _(make-dependency: keep #7 as the canonical metric D)_
- #71, #72: #71 (DECISION: internal shard map and partition count decoupled from the 16384 compatibility slots) and #72 (DECISION: keyspace partition count as dua _(merge: combine #71 and #72 into a single DECISION )_
- #52, #53, #92: The 'two compression-decision issues' are #53 (DECISION: default codec = zstd low-level; LZ4/none as policy options) and #92 (DESIGN: off-path per-val _(make-dependency: keep #52 as the parent strategy a)_
- #87, #93: #87 (RESEARCH: continuously-reported online Belady-MIN gap metric) and #93 (TASK: offline Belady-MIN and learned-Belady oracle in the benchmark harnes _(keep-separate-but-cross-link: online (#87, on the )_
- #95, #97: #95 (DESIGN: conformance, differential, fuzz, property, and DST testing stack) is the umbrella testing-stack design that already enumerates 'different _(make-dependency: keep #95 as the umbrella that def)_

## Flagged issues and their headline finding

Full findings are in each issue's audit comment. Summary of the non-valid set:

| Issue | Verdict | Headline |
| --- | --- | --- |
| #5 | has-a-bug | The 'Auditor notes' assert a tree-wide coverage guarantee using a wrong issue count. The note says 'All 90 issues are present in the M0/M1/M2 issue ma |
| #7 | has-a-bug | The issue states Valkey I/O-thread gains as "~380K to ~1.2M RPS", but the cited claim valkey-io-threads-throughput records the low end as ~360K (7.2)  |
| #8 | has-a-bug | The issue describes Redis maxmemory accounting as 'allocator introspection' and pairs the citation with jemalloc `stats.allocated`. But the cited clai |
| #11 | too-large | The issue compounds three independent non-goals that share only a tenet justification. The title joins three topics ('fork()+copy-on-write snapshottin |
| #15 | has-a-bug | The HELLO acceptance criterion names the first server-info map field 'name', but real Redis keys this field 'server' (value e.g. "redis"); the map is  |
| #18 | has-a-bug | claim citation: Cited in acceptance criterion #5 to validate 'RESP2 and RESP3 null/error encodings', but the claim only records null encodings ($- |
| #22 | has-a-bug | The body defers the full ACL engine to a config issue. It states the full ACL engine (@category / %R%W / selector + aclfile) is 'scoped here but defer |
| #23 | has-a-bug | The issue states SET ... IFEQ is a Redis 7.0 idiom, but the IFEQ/IFNE/IFDEQ/IFDNE compare-and-set options were introduced in Redis 8.4.0 (verified aga |
| #28 | has-a-bug | #28 states the persistence writer 'submits fixed-buffer writes on the same per-shard ring' and lists acceptance criterion 'Registered buffers shared b |
| #29 | too-large | Title joins two independent concerns with 'and': the cross-shard coordinator mechanism (a concurrency design) and the transaction/scripting surface (a |
| #33 | has-a-bug | The issue cites [seize-vs-epoch] to support adopting a FASTER-style global-epoch + thread-local + trigger-action drain list. The claim's recorded valu |
| #35 | too-large | The title joins three distinct topics ('Hash table, data-structure encodings, and per-key object layout') and the body compounds at least five separab |
| #37 | needs-more-info | The issue self-blocks via a 'BLOCKED ON INPUT' banner and defers every substantive decision: it explicitly declines to assert numeric memory/CPU delta |
| #39 | too-large | The title joins two independent data-structure encodings ('intset and HyperLogLog'), and the 8-item acceptance list spans three separable concerns: (1 |
| #40 | has-a-bug | The issue cites [redis-quicklist-node-32-bytes] to justify deriving/synthesizing the ql_nodes field. That claim's recorded value is '32 bytes per quic |
| #41 | has-a-bug | The prose repeatedly names the shipped allocator as 'tikv-jemallocator (jemalloc 5.3.1)' and asserts it is 'what Redis/Valkey run, so behavior is well |
| #43 | has-a-bug | The issue states the reclaimer is gated on mem_fragmentation_ratio (RSS/used_memory) and pairs that with Redis-compatible thresholds (lower 10%, upper |
| #44 | has-a-bug | The issue leans on [jemalloc-thp-default] to justify MADV_NOHUGEPAGE as merely mirroring the existing jemalloc default. But the claim value is 'opt.th |
| #45 | has-a-bug | The accounting sentence cites redis-maxmemory-accounting to support the claim that accounting against 'allocator-attributed bytes' makes the ceiling t |
| #47 | has-a-bug | The issue asserts SIEVE preserves a 'stack-like property', but the cited claim sieve-loc-and-stack-property has verdict=corrected and its value explic |
| #48 | has-a-bug | The issue universalizes the SIEVE single-visited-bit hot path to ALL policies: it claims 'the per-entry state every policy touches on a hit is a singl |
| #51 | has-a-bug | The issue cites [keydb-subkey-expire] to claim KeyDB's per-element TTL implementation was reverted and that there is no production design to borrow. T |
| #52 | has-a-bug | claim citation: Issue states RocksDB 'amortizes dictionary and framing overhead across a 4KB block,' but the claim records that the 4 KB block is  |
| #54 | has-a-bug | The issue cites [zstd-rust-crate-version] as the evidence that pure-Rust zstd 'currently lags the C reference on speed, ratio, and configurability' an |
| #56 | has-a-bug | The Prior-art section attributes the 'stable, in-place-updatable record size' constraint to [garnet-narrow-waist-api], but that claim records only the |
| #58 | has-a-bug | The 'Borrow Garnet's safe defaults' bullet groups four defaults and cites four claims, but for 'checkpoint off out of the box' it relies on [garnet-ch |
| #60 | has-a-bug | Issue #60 declares it 'depends on the shared-nothing single-writer-per-shard architecture from #1 and the index from #11.' #11 is titled '[NON-GOAL]:  |
| #64 | has-a-bug | The issue states the FASTER cache-line hash bucket has '8 entries' (Prior art) and proposes to 'keep the 64-byte/8-entry bucket' (Open decisions), cit |
| #65 | has-a-bug | The issue cites keydb-flash-190gb-benchmark to substantiate negative operational claims about RocksDB compaction, but that claim records a POSITIVE re |
| #66 | has-a-bug | The central write-amplification-vs-LSM argument is cited to [keydb-flash-rocksdb], but that claim's recorded value is 'RocksDBStorageProvider with dat |
| #69 | has-a-bug | The rationale equates the per-slot shard with the per-core execution shard as a cost-free 1:1 identity. This is technically inaccurate: a real machine |
| #71 | has-a-bug | #71 and #72 reach directly opposite conclusions on the single load-bearing number (the internal partition count P), and each cites the other as author |
| #72 | has-a-bug | Issue #72's recommendation (16384 = dual-purpose unit, internal partition == client slot, '1:1 with zero translation', 'no remapping layer') is the ex |
| #75 | has-a-bug | The issue characterizes cluster-migration-barrier as a slot-resharding throttle, but that config governs an unrelated feature: the minimum number of r |
| #82 | too-large | The issue compounds two separable decisions with disjoint option sets and outputs. Concern A (mode dispatch: clap subcommands vs argv[0] symlink) is a |
| #83 | has-a-bug | The only cited claim id is the literal placeholder `cite-needed` (exists=false), appearing 4 times as `[cite-needed]` against every version-pinned pri |
| #84 | too-large | The issue compounds at least four separable concerns: (a) the reproducible cross-build matrix on cargo-zigbuild, (b) the install/distribution surface  |
| #85 | has-a-bug | The INPUT GAP block claims the source research and claims.yaml are missing: it says the docs 'do not exist on disk; no claims.yaml for IronCache was f |
| #87 | has-a-bug | The issue overstates what [lhd-hit-density] supports. It says hit-density eviction "is reported to track optimal closely on real workloads [lhd-hit-de |
| #88 | has-a-bug | The bracketed citation [ng-reject-neural-hotpath] is treated as a prior-art claim id but no such claim exists in docs/prior-art/claims.yaml (packet ci |
| #89 | has-a-bug | The issue asserts the hit-ratio-hurts-throughput effect as universal and unconditional ('a higher hit ratio can lower throughput'; 'The advisor must t |
| #92 | has-a-bug | Prose overstates lz4_flex safe-mode parity with C lz4. The issue says lz4_flex 'stays within ~10% of C in safe mode,' but the cited claim records safe |
| #95 | has-a-bug | The prior-art bullet states Valkey 'is wire-identical to Redis 7.2 RESP2/RESP3 [valkey-resp-identical]', but the claim's recorded value is explicitly  |
| #97 | has-a-bug | The Diff bar row cites [error-string-catalog], but this claim id does not exist in docs/prior-art/claims.yaml (cited_ids shows exists=false, value=nul |
| #99 | has-a-bug | The issue points an implementer to #12 and #68 for 'the consensus implementation itself.' #12 is a NON-GOAL ('[NON-GOAL]: Strong consistency / zero wr |

The 55 issues not listed above were assessed **valid** (individually coherent,
citations correct, right-sized). Tree-level gaps are addressed by the new
issues filed above, not by changes to those issues.
