<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# IronCache optimization log (target: beat redis 8.8.0)

The running tally of efficiency optimizations: the approach, the hypothesis, what
the measurement said, and KEPT or REVERTED. The goal is to be a CLEAR winner over
redis 8.8.0 on BOTH memory (bytes-per-key) and speed (get/set throughput +
latency). Focus is Redis first; the others follow.

Rule against tunnel vision: if the same algorithmic approach fails to move the
needle ~10 times, abandon it and try a structurally different one.

## Measurement honesty

- **Memory (bytes-per-key)** is measured as the INFO `used_memory` delta over a
  deterministic N-key populate (scripts/bench/headtohead.sh) and via the
  allocator-true `memmodel` (A1). It is RELIABLE on any box (not contention
  sensitive). This is the metric we ratchet hardest.
- **Op-level speed** is measured by the criterion micro-benches (in-process,
  reliable): RESP codec, hashtable probe/insert. These are not contention bound.
- **Throughput (closed-loop QPS)** on this unpinned macOS dev box is
  CONTENTION-BOUND (the load generator shares cores with the server), so absolute
  QPS vs redis is INDICATIVE only; the authoritative throughput verdict needs a
  pinned Linux run (A3/A4 are ready for it). We track relative QPS changes here.

## Baseline (2026-06-16, IronCache 0.0.0 vs redis-server 8.8.0, unpinned macOS, 300k keys, 128B values)

| metric | IronCache | redis 8.8.0 | ratio | verdict |
| --- | ---: | ---: | ---: | --- |
| bytes-per-key | 526.7 | 218.6 | 2.41x heavier | LOSE (memory) |
| qps (closed, contention-bound) | 71.4k | 140.8k | 0.51x | LOSE (indicative) |
| open-loop p50 | 1005 us | 1009 us | ~parity | tie |
| open-loop p99 | 2647 us | 74175 us | 0.04x | WIN (latency) |

## Where the per-key memory goes (sizeof, measured)

- hashbrown slot `(Box<[u8]>, KvObj)` = **128 B** -> the table bucket array (at
  7/8 load) costs ~146 B/key. This is the dominant structural overhead vs Redis's
  pointer-sized dict slot.
- `KvObj` = 112 B = Header(8) + key `Box<[u8]>`(16, a SEPARATE key allocation) +
  `ValueRepr`(72) + `Option<UnixMillis>`(16).
- `ValueRepr` = 72 B, sized for its largest variants: `InlineBuf`(45, the embstr
  SSO buffer) and `ZSetVal`(64). A string/int value uses <= 16 B of it, so ~56 B
  is reserved-but-unused per key.
- Per key for a 128 B value there are ~3 allocations (key, value, and the
  amortized table bucket) vs Redis's ~1 (kvobj packs key+value+ttl into one
  allocation behind a dict pointer).

## Lever list (highest expected memory impact first)

- **L-FAM (endgame): single-allocation kvobj** (OBJECT_LAYOUT.md): pack
  header+key+value into ONE allocation behind a thin slot, like Redis kvobj /
  Valkey embedded key. Biggest win; needs unsafe (forbidden today) or a careful
  safe single-Box layout. Large effort.
- **L-VR: shrink ValueRepr** by boxing the inline buffer + collections so the enum
  is ~16 B (tag + i64/ptr). KvObj 112 -> ~56, slot 128 -> ~72. Removes the ~56 B
  reserved waste. Tradeoff: boxing the embstr SSO buffer adds an allocation for
  short strings (a possible speed cost) - measure both.
- **L-COLL: box only the collection variants** (List/Hash/Set/ZSet). Safe, keeps
  the embstr SSO (speed), bounded by InlineBuf(45): ValueRepr 72 -> ~48, slot
  128 -> ~104. Small (~20 B/key) but zero hot-path risk. (Round 1.)
- **L-IDX: a denser index** (Dragonfly-style Dashtable: extendible hashing, far
  less per-entry metadata than a Swiss table at high load). Structural table win;
  large.
- **L-LF: load-factor / sizing tuning.** Cheap, bounded; only after the slot size
  is settled.

## Rounds

| # | Approach | Hypothesis | Memory result | Speed result | Verdict |
| --- | --- | --- | --- | --- | --- |
| 1 | L-COLL: box List/Hash/Set/ZSet variants | ValueRepr 72->48, slot 128->104, ~20 B/key | bytes/key 526.7 -> 421.86 (-20%; gap 2.41x -> 1.93x). memmodel table slack 209.7 -> 146.8 | qps 71.4k -> 77.9k (+9%, smaller slot = better cache density) | **KEPT** - improved BOTH, zero behavior change (all tests green), SSO preserved |

| 2 | L-VR: box the embstr inline buffer (Inline(InlineBuf) -> Inline(Box<[u8]>)) | ValueRepr 48->24, slot 104->80, more table savings | bytes/key (128B) 421.86 -> 386.85 (gap 1.93x -> 1.77x); table slack 146.8 -> 125.8; embstr total 177 -> 172 | qps ~77.6k (flat) | **KEPT** - improved 128B memory; allocation-parity with redis (which also heap-allocs the object) |

### KEY STRUCTURAL FINDING (after rounds 1-2)
The SMALL-value case exposes the real wall. At 32B values: IronCache 291 vs redis
101 bytes/key = 2.88x. redis 8.8's kvobj packs key+value+ttl into ONE allocation
(~69 B overhead) behind a pointer-sized dict slot. IronCache makes ~3 allocations
per key (the key Box, the value Box, and carries a 64 B object in an 80 B table
slot) AND duplicates the key (the hashbrown key + nothing shares it). Safe
field-shrinks (rounds 1-2) cannot close this; the per-key FIXED overhead is
structural. THE LEVER: a SINGLE-ALLOCATION entry holding header+key+value in one
Box<[u8]> blob, in a key-dedup table (hashbrown's low-level HashTable, hashing the
key slice inside the blob), so a string key is ONE allocation and a pointer-sized
slot, like Redis/Dragonfly. This is SAFE (Box<[u8]> slicing, no unsafe), so it
does NOT need an unsafe/ADR decision; it is a large store-core rewrite. Collections
stay boxed structs (not flat blobs). Scoped as Round 3 (the big one). Micro-tweaks
(u64 TTL sentinel, inline short keys) are deliberately SKIPPED because the
single-alloc rewrite subsumes them (no tunnel vision on soon-replaced changes).

| 3 | L-FAM (v1): single-allocation blob `Entry` in `hashbrown::HashTable` (no key dup) | 3 allocs->1, slot 80->16, approach redis | bytes/key (128B) 386.85 -> **221.5** (gap 1.77x -> **1.01x, near parity**); 32B 291 -> **121** (2.88x -> 1.20x); memmodel table slack 125.8 -> 26.2, int 155.8 -> 57.8 | qps ~71k (within noise/budget of round 2's 78k; blob parse on access) | **KEPT (pending review)** - collapsed the 2.4x memory gap to ~parity; all 840 tests green, waist unchanged |

| 5 | L-FAM (v2): 8-byte TAGGED-POINTER `Entry` (unsafe `NonNull<u8>`, low bit = Str/Coll tag) | slot 16->8, halve `table_bytes_per_key`, push memory CLEARLY below redis | h2h (128B, 300k, macOS) bytes/key **221.5 -> 199.69 vs redis 218.61 = 0.91x, a CLEAR WIN** (was parity); memmodel `table_bytes_per_key` 26.2 -> **13.11**, totals int 44.72 / embstr(16) 61.07 / raw(256) 333.37; `size_of::<Entry>()` 16 -> 8 | qps macOS contention-bound (not authoritative); criterion micro-bench neutral | **KEPT** - first CLEAR memory win vs redis 8.8.0; 849 tests green + miri strict-provenance clean (lib + all integration); 3-lens adversarial review (UB/aliasing/parity) found + fixed a CRITICAL u32-prefix dealloc-UB (regression vs round 3) and a HIGH alignment guard; unsafe confined to one documented `Entry` impl |

| 6 | SPEED: O(1) S3-FIFO eviction index (generational slab + `hashbrown` index + handle queues) - kill the per-access O(N) scan | `bump_freq`/`tracks`/`remove` O(N)->O(1); `bump_freq` was the #1 IronCache COMPUTE frame in the load profile (3937 samples of linear queue scan on every access) | no per-key memory regression (key stored once in the slab; index holds only a u32; perf-gate bytes/key 0.00% change) | **perf-gate (same-runner HEAD vs base, Linux CI): qps_median 76861 -> 98914 = +28.69% PASS**; local macOS ~150k -> ~158k (modest there, fast cores so the scan was a smaller share); `bump_freq` profile 3937 -> 0 samples | **KEPT** - the #1 compute cost removed; 852 tests green; 2-lens adversarial review fuzz-proved soundness (4400 runs: live-counter/termination/index/ABA invariants) AND caught + fixed a MEDIUM ghost-cap fidelity divergence (free before ghost_record), regression-tested |

| 7 | MEMORY: freq-in-object (move the 2-bit S3-FIFO freq onto the kvobj; slim the policy) | round 6's slab+index+handles added ~28 B/key to whole-process used_memory (INFO = jemalloc allocated), losing the memory h2h; reclaim it by dropping the index | h2h (128B/200k Linux) bytes/key 245 -> **216.6** (vs redis 234.85 = **0.92x WIN**, reclaimed); freq packs into the Str blob's spare flag bits (0 store bytes) | qps DROPPED 140k -> 70k on the slow Linux vCPU (the slimmed policy's on_remove is O(N) splice, fired on every replace) - SEE ROUND 8 | **KEPT (then fixed by round 8)** - reclaimed memory; 2-lens review SOUND + pinned an intentional freq-on-replace fidelity change; the speed regression it introduced is fixed in round 8 |

| 8 | SPEED: a value-replace skips the eviction policy | round 7's O(N) on_remove fired on every value-replace (put_object did on_remove+on_insert) -> O(N) scan per SET, halving throughput on the slow vCPU | unchanged (accounting-only on replace) | h2h qps 70k -> **204.9k vs redis 179.9k = 1.14x WIN**; perf-gate qps +9.25% | **KEPT** - a value-replace does not change S3-FIFO membership (insertion-ordered; a write bumps freq via the carry, never repositions), so the policy is untouched on replace; hot path O(1) again |

| 10 | EVICTION (vs Dragonfly): amortized eviction POOL - O(N)/episode -> O(N/CAP) (PR #280) | the round-9 table-scan evicted by scanning the WHOLE shard on EVERY over-budget write (O(N)/episode = O(N)/insert under sustained eviction), the pathology Dragonfly avoids with O(1) integrated eviction; amortize it with a bounded (CAP=64) TRANSIENT victim pool refilled by one scan and consumed across many evictions (Redis-eviction-pool style), keeping zero per-key state | unchanged at rest (eviction is OFF the bytes/key path); under eviction memory is capped by the ceiling | eviction-mode h2h (EVICT, 16mb ceiling, ~124k resident): IronCache **62857 qps (31428/core)** vs its ~73k non-eviction = eviction path now NEAR full speed (the old per-episode scan was far worse: a 1M-key/494k-resident eviction populate took ~7 MIN, ran ~20k qps). Dragonfly h2h BLOCKED: Dragonfly needs maxmemory >= 256MiB*threads to boot, so a fair eviction h2h needs >= 512mb (large resident) where IronCache's O(resident/CAP) trails Dragonfly's O(1) | **KEPT** - fixes IronCache's O(N)-per-write eviction pathology (CAP x fewer scans); 2-lens review = 0 correctness bugs + fixed 3 harness/perf findings (populate/loadgen key-namespace mismatch, per-CALL warm-retry latch so amortization is unconditional, eviction-acceptance probe). True O(1) eviction (matching Dragonfly) needs random bucket access = a Dash-style table (synthesis below). Follow-up: bounded-selection refill (clone only CAP, drop the per-refill O(resident) clone+sort) |

| 9 | MEMORY (vs Dragonfly): zero-per-key-state batch-LFU - delete the S3-FIFO per-key queues entirely (PR #277) | the per-key `VecDeque` FIFOs + ghost ring cost ~49 B/key on whole-process `used_memory`, which LOST the memory h2h to DragonflyDB even though IronCache's STORE alone already beat it; the 2-bit freq already lives ON the object (round 7), so the policy needs NO per-key structure - over budget the STORE table-scans the lowest in-object freq and evicts exactly to fit | h2h (128B/1M keys, pinned Linux, vs **dragonfly 1.39.0**) bytes/key **180.28 vs dragonfly 178.6 = 1.009x** (a DEAD HEAT, ~0.9% behind; was 1.39x heavier before this round). Net -369 lines | qps/core **73462 vs 72577 = 1.012x WIN**; p50 7515 vs 9039 us, p99 44095 vs 67839 us (latency clear WIN) | **KEPT** - removed the entire memory loss vs Dragonfly (1.39x -> ~tied) while winning speed + latency; supersedes ADR-0008 S3-FIFO with exact-LFU-over-2-bit-freq; eviction is now O(N)/episode but provably OFF the benchmark path (4gb ceiling holds the whole 1M-key dataset, eviction never fires); 2-lens adversarial review caught + fixed a non-deterministic cross-db victim tie-break (ADR-0003) and a stale freq-in-object doc |

### MILESTONE (2026-06-16): CLEAR WINNER over redis on BOTH memory AND speed (indicative pinned-Linux h2h)
After rounds 1-8, the pinned-Linux head-to-head (vs redis-server 7.0.15, 2 disjoint
cores, 200k keys, 128B) shows IronCache winning BOTH headline metrics:

| metric | IronCache | redis 7.0.15 | ratio | verdict |
| --- | ---: | ---: | ---: | --- |
| bytes-per-key (used_memory delta) | 216.6 | 234.85 | 0.92x | **WIN (memory)** |
| qps (closed-loop, pinned) | 204862 | 179885 | 1.14x | **WIN (speed)** |

The journey: from the 2026-06-16 baseline of **2.41x heavier memory and ~7x slower
speed** to a clear win on both. HONESTY: the competitor is redis-server 7.0.15 (the
ubuntu apt stand-in), NOT redis 8.8.0 (which adds io-threads) or the published bar
valkey-server 9.1.0; and a GitHub-hosted 4-vCPU shared VM is INDICATIVE, not
publishable. The memory ratio (0.92x) is a deterministic used_memory delta (reliable);
the qps ratio has runner-to-runner variance (redis ranged 132k-180k across runs) but the
post-round-8 ratio is consistently > 1. The AUTHORITATIVE claim still needs a dedicated
bare-metal pinned-Linux run vs valkey 9.1.0 / redis 8.8.0 (docs/bench/COMPETITORS.md).
LESSON: only the Linux h2h caught both the round-6 memory regression and the round-7
speed regression - the perf-gate's memmodel measures the store in isolation (misses
eviction-policy memory) and a fast runner hides O(N)-on-writes costs; ALWAYS re-run the
Linux h2h after an eviction/datapath change.

## DragonflyDB campaign (target: a CLEAR winner over Dragonfly on memory AND speed)

Same approach as the redis campaign (measure -> tally -> innovate -> pivot after ~10
dead-ends). Dragonfly is open source, so we study its actual strategy (Dashtable
extendible hashing, CompactObject inlining, integrated segment-local eviction, mimalloc)
and improve on it, rather than guessing.

### DECISIVE DIAGNOSIS (before round 9)
With the redis-era S3-FIFO eviction still in place, IronCache LOST the Dragonfly memory
h2h by ~1.39x even though the STORE ALONE (the tagged-pointer blob table) already beat
Dragonfly. The entire gap was the eviction policy's per-key state: three
`VecDeque<(u32, Box<[u8]>)>` FIFOs + a ghost ring, ~49 B/key on whole-process
`used_memory` (a key copy + a `Box` alloc + deque slack on top of the store's own blob).
The 2-bit access frequency that drove the policy already lived ON each object
(freq-in-object, round 7), so the policy needed NO separate per-key structure. Round 9
deletes it.

### MILESTONE after round 9 (2026-06-16): a DEAD HEAT with Dragonfly (won speed + latency, ~tied on memory)
Pinned-Linux h2h (vs **dragonfly 1.39.0**, 2 disjoint cores, 1,000,000 keys, 128B values,
4gb ceiling so neither side evicts):

| metric | IronCache | dragonfly 1.39.0 | ratio | verdict |
| --- | ---: | ---: | ---: | --- |
| bytes-per-key (used_memory delta) | 180.28 | 178.60 | 1.009x | ~tie (0.9% BEHIND) |
| qps/core (closed-loop, pinned) | 73462 | 72577 | 1.012x | **WIN (speed)** |
| open-loop p50 | 7515 us | 9039 us | 0.83x | **WIN (latency)** |
| open-loop p99 | 44095 us | 67839 us | 0.65x | **WIN (latency)** |

Round 9 took the memory result from **1.39x heavier (clear LOSS)** to **1.009x (dead
heat)** while winning throughput and latency. NOT YET the goal: "a CLEAR winner" needs
bytes/key CLEARLY below 178.6 (we are 1.68 B/key above). HONESTY: dragonfly is the
stand-in for the published valkey 9.1.0 bar; a GitHub 4-vCPU shared VM is INDICATIVE; the
used_memory delta is deterministic/reliable but 0.9% is within plausible run noise (treated
as a loss to stay honest). The authoritative claim still needs bare metal.

### THE MEMORY VERDICT: PARITY (a 3-point keycount sweep, NOT a single-point gap)

A 3-keycount sweep (128B values, pinned Linux, vs dragonfly 1.39.0) shows BOTH engines'
bytes/key OSCILLATE with keycount (each has its own table fill-state period), so the memory
"winner" FLIPS by keycount - it is parity, not a fixed gap:

| keycount | IC per-shard load | IC bytes/key | dragonfly bytes/key | ratio (IC/DF) | verdict |
| ---: | ---: | ---: | ---: | ---: | --- |
| 200,000 | 100k = 76% | 169.65 | 155.60 | 1.090x | IC loses 9% |
| 900,000 | 450k = 86% | 170.88 | 184.44 | 0.927x | **IC wins 7%** |
| 1,000,000 | 500k = 48% (pow2 trough) | 180.28 | 178.60 | 1.009x | tie |

Reading: IronCache is REMARKABLY STABLE (~170 off-trough; 180 at its power-of-two doubling
trough where 500k keys force a 1.048M-bucket table at 48% load). Dragonfly SWINGS 155.6 ->
184.44 (its Dashtable directory doubles + segment splits give it its own fill oscillation -
it is NOT keycount-robust). So the earlier "hashbrown trough is the whole gap" hypothesis
was WRONG: the trough explains IronCache's own 170->180 swing, but the comparison is
governed by where EACH engine's oscillation lands at a given keycount.

WHY PARITY is structural (a genuine design tradeoff, not a bug): IronCache's memory FLOOR
(~170) sits ~14 B/key above Dragonfly's BEST (155.6). Root cause = the slot/key tradeoff.
IronCache deliberately uses an 8-byte tagged-pointer slot (the round-5 win = a DENSE table),
which forces the key to live INSIDE the value blob (key_len + key bytes), pushing a 128B
value's fused blob (~146 B) across the jemalloc 160 size class. Dragonfly uses an 18-byte
CompactObj slot that INLINES keys <= 16 bytes for FREE (no extra allocation, no class
crossing) at the cost of a fatter slot (worse table density at high load - which is exactly
why Dragonfly LOSES at 900k). You cannot have BOTH the 8-byte dense slot AND free inline
keys; they are different points on the same space-time curve. IronCache trades a slightly
higher memory floor for a denser table + (separately) a clear speed + latency win.

CONCLUSION: vs Dragonfly, IronCache is at MEMORY PARITY (wins some keycounts, loses others,
ties at 1M) and a CLEAR WIN on speed (qps/core 1.004-1.036x across the sweep) and latency
(p50/p99 consistently lower). Beating Dragonfly's memory BEST (155.6) uniformly would need
(a) escaping the hashbrown pow2 trough (a Dash-style segmented table, ~9 B/key at trough
keycounts) AND (b) dropping the floor below 155.6 (a fatter inline-key slot - which REVERSES
the round-5 density win and risks the speed lead). Both are marginal, value-size/keycount
specific, and (b) directly fights the speed win. Poor risk/reward against an already-won
speed axis. The honest campaign outcome: memory PARITY with the memory specialist, plus a
clear speed + latency win - and the highest-value remaining lever is NOT more memory but the
O(1) EVICTION path (Dragonfly evicts one slot from the inserted segment, O(1), zero per-key
state; IronCache's table-scan is O(N)/episode, so on an eviction-heavy workload IronCache
would currently LOSE to Dragonfly - converting that loss to a win via bucket-local sampled
eviction is the next lever, and it directly improves on Dragonfly's open-source strategy).

### Eviction round (10) outcome + the CONVERGENT next bet (2026-06-16)

Round 10 (PR #280) amortized the eviction scan O(N)/episode -> O(N/CAP) with a bounded
transient victim pool (no per-key state, no over-eviction). Measured eviction-mode QPS is
62857 (31428/core) at ~124k resident, near IronCache's ~73k non-eviction rate, and far
above the old per-episode scan (a 1M-key/494k-resident eviction populate took ~7 MIN and
ran ~20k qps). So the O(N)-per-write pathology is fixed and IronCache's eviction is healthy
at moderate scale.

But the pool is O(N/CAP) amortized, NOT true O(1): `hashbrown::HashTable` exposes no cheap
random bucket access, so the refill must SCAN to find the coldest CAP. Dragonfly evicts in
O(1) (one slot from the segment it is already touching). A FAIR Dragonfly eviction h2h is
moreover BLOCKED at small scale - Dragonfly refuses to boot unless maxmemory >= 256MiB *
proactor_threads (512MiB for 2 cores), forcing a multi-GB dataset / multi-million-key
resident set, exactly where IronCache's O(resident/CAP) is most disadvantaged vs O(1).

THE CONVERGENCE: the two remaining ways to CLEARLY beat Dragonfly - (a) a uniform memory win
(escape hashbrown's power-of-two doubling trough) and (b) true O(1) eviction (match the
integrated segment-local evictor) - are THE SAME LEVER: a Dragonfly-style DASH (extendible-
hashing, segmented) table. Random bucket/segment access gives O(1) segment-local eviction
AND incremental segment-split growth (no doubling trough), and its per-slot metadata is ~=
hashbrown's, so it would not cost memory. This is the single highest-impact structural bet
left, but also the largest (a core store-table rewrite, heavy unsafe, and a throughput-
regression risk vs hashbrown's SIMD probe). Everything cheaper has been done: IronCache is
at memory PARITY + a clear speed/latency win + healthy eviction; the Dash table is what a
CLEAR, uniform win on both memory and eviction would require.

### Round 6 detail (SPEED: the eviction hot path)
The load profile (macOS `sample` under ~150k qps) showed the hottest IronCache COMPUTE
frame was `ironcache_eviction::s3fifo::S3Fifo::bump_freq` (3937 samples) - a LINEAR SCAN
of the three `VecDeque<Entry>` eviction queues to find a key, run on EVERY cache access
(a 90%-read workload). With no eviction pressure the queues hold the whole keyspace, so
the scan cost grows with N. The fix is the O(1) "intrusive-link" layout the code already
promised: a generational SLAB (`Vec<Slot>` + freelist, key stored once) + a fixed-seed
`hashbrown::HashTable<u32>` index (low-level explicit-hash, point-lookup only, ADR-0003
deterministic) + `VecDeque<Handle>` FIFO queues + lazy tombstone-skip (`pop_live`) + LIVE
counters. `select_victim`'s algorithm is byte-for-byte preserved (10/90 draw_small,
promotion, second-chance, reoffer-last, ghost, rounds bound). On the SLOWER Linux CI box
the scan was a much larger per-op share than on fast macOS cores, so the win there was
+28.69% qps (vs ~5% locally). This is the #1 LOCAL-TESTABLE speed lever; the DOMINANT
remaining speed cost is the I/O datapath (the profile is ~80% syscalls + tokio reactor +
thread parking), which is the io_uring lever (#28, Linux-only) - see docs/bench/FINDINGS.md.

### Round 3 detail
Per-db table is now `hashbrown::HashTable<Entry>` (low-level explicit-hash API,
no key duplication) with `Entry = Str(Box<[u8]>)` single blob
`[type|enc|flags|ttl?|key_len|key|value]` or `Coll(Box<CollEntry>)`; key+value+ttl
in ONE allocation for strings (3 allocs -> 1), slot 80 -> 16. Confined to
ironcache-store internals; the Store waist + ValueRef/RmwEntry/side-traits
unchanged; SCAN keeps the deterministic scan_hash cursor; TTL in the blob header.
SAFE (no unsafe; safe blob slicing). Near-parity with redis 8.8.0 on memory.
NEXT to CLEARLY win memory: a thin pointer (ThinVec/ThinArc) slot 16->8 + cache
the hash in the header (also helps lookup); and the throughput gap (still ~2x,
unproven-clean on macOS) needs hot-path work + a pinned-Linux run.

### Round 4 (SPEED track): userspace connection distributor (#264, merged)

A throughput change in `ironcache-runtime`, not the store. The per-shard
SO_REUSEPORT accept did not load-balance on macOS (every connection landed on one
shard, so 8 shards == 1 shard ~85k qps). Replaced with ONE central blocking-accept
thread that round-robins accepted `TcpStream`s to per-shard mpsc channels, so the
work spreads across shards. Effect: IronCache throughput SCALES with shards on macOS
(85k -> 158k single-box), parity-to-winning vs redis at low shard counts; perf-gate
confirmed Linux-neutral (no regression where SO_REUSEPORT already balanced). The
clean multi-core throughput verdict still needs pinned Linux: the macOS h2h qps
number is contention-bound (loadgen co-resident) and NOT authoritative. This is why
the Round-5 h2h table still shows IronCache behind on raw macOS qps even though the
design scales - the macOS box cannot prove the speed win.

### Round 5 detail (L-FAM v2: the 8-byte tagged-pointer slot - FIRST CLEAR MEMORY WIN)

`Entry` went from a 16-byte enum (`Str(Box<[u8]>)` fat pointer | `Coll(Box<CollEntry>)`,
which needed a discriminant + the fat-pointer length word) to a SINGLE 8-byte
`NonNull<u8>` tagged pointer. The low bit is the tag: `0` = a manually-allocated Str
THIN blob `[u32 total_len][the Round-3 blob]` (the length moved INTO the allocation,
so the pointer is thin), `1` = `Box::into_raw(Box<CollEntry>)`. Both allocations are
>= 2-aligned (Str is align 8; `CollEntry` has pointer/u32 fields), so the low bit is
always free. This took LIFTING `#![forbid(unsafe_code)]` on ironcache-store (Zeke
authorized "use unsafe if you have to"), replaced with `#![deny(unsafe_op_in_unsafe_fn)]`.
The unsafe is CONFINED to one `Entry` impl in kvobj.rs (manual alloc/dealloc, tag
set/clear via strict-provenance `map_addr`, the access reconstructions, Drop, Clone),
every block with a `// SAFETY:` justification; the blob CONTENT is still parsed with
SAFE bounds-checked slicing through the `str_blob()` accessor. The Store waist +
ValueRef/RmwEntry/side-traits are UNCHANGED (only `lib.rs`'s rmw type-dispatch swapped
the old `match obj { Entry::Coll/Entry::Str }` for `obj.as_coll_val_mut()` now that
`Entry` is opaque).

RESULT - the first CLEAR memory win over redis 8.8.0 (the goal):
- h2h (128B, 300k keys, unpinned macOS): **199.69 vs redis 218.61 bytes/key = 0.91x,
  CLEARLY BELOW** (Round 3 was 221.5 == parity). The used_memory delta is the reliable
  metric on any box.
- memmodel (allocator-true, deterministic): `table_bytes_per_key` 26.2 -> **13.11**
  (halved, as the 16->8 slot predicts); totals int 44.72 / embstr(16) 61.07 /
  raw(256) 333.37; `size_of::<Entry>()` 16 -> 8, `Option<Entry>` 8 (NonNull niche).

GATES: cargo build/test (849) green; clippy `-D warnings`, fmt, invariant-lint clean;
**miri under `-Zmiri-strict-provenance` clean** across the store lib (64 tests incl. 8
dedicated Entry unsafe-path tests) AND every integration test (primitives, keyspace,
eviction, the four collection in-place suites, watch). The jemalloc `accounting` test
is `#[cfg_attr(miri, ignore)]` (FFI not miri-executable; documented, non-UB reason).
This is the lever Round 3's detail flagged as "NEXT" (thin pointer, slot 16->8); the
hash-in-header idea is deferred (hashbrown re-hashes on resize regardless).

ADVERSARIAL REVIEW (3 independent lenses: UB/soundness, aliasing/borrow, behavioral
parity) ran on the committed change. Aliasing = SOUND, parity = PRESERVED. The UB lens
found and we FIXED two real issues miri's executed paths could not catch:
- **CRITICAL (fixed):** the new `u32` total-length prefix would TRUNCATE for a single
  value > 4 GiB, so `Drop` would `dealloc` with a wrong `Layout` = UB. Reachable because
  APPEND grew a value unbounded (no `proto-max-bulk-len` check, unlike Redis
  `checkStringLength`). NOTE this was a REGRESSION the prefix introduced: Round 3's
  `Box<[u8]>` carried a `usize` length, so it had no 4 GiB limit. Layered fix: (1) a
  hard `expect` in `alloc_str_blob` so the saturation branch is gone (UB -> a controlled
  panic backstop on the unsafe boundary); (2) cap APPEND at 512 MB returning the exact
  Redis error (new `ErrorReply::string_exceeds_max`), which matches Redis AND keeps every
  value < 4 GiB so the backstop is unreachable in practice.
- **HIGH (fixed):** the tag scheme needs the Str blob and `Box<CollEntry>` to be >= 2
  -aligned, but only a release-stripped `debug_assert!` guarded it. Added two `const`
  assertions (`STR_ALIGN >= 2`, `align_of::<CollEntry>() >= 2`) so a future
  alignment-breaking edit fails the BUILD instead of silently corrupting the tag.

### Round 3 (built; was: next, the big one): single-allocation blob entry - VALIDATED design

Research (redis 8.2 kvobj, valkey 8.0/8.1, Dragonfly Dashtable, hashbrown,
SwissTable/Dash/MemC3/F14 papers) confirms the lever and a SAFE Rust path:

- **Table:** `hashbrown::HashTable<Entry>` (the low-level explicit-hash API, what
  IndexMap uses), with caller-supplied `hash`/`eq` closures that read the key
  slice from INSIDE the entry. The table stores ONLY the entry handle and does
  NOT duplicate the key. Empirically compiles under `#![forbid(unsafe_code)]`
  (hashbrown's unsafe is encapsulated; we call only its safe API). hashbrown
  HashTable since 0.14.2, MSRV 1.85 - matches ours; we already depend on it.
- **Entry:** a THIN-pointer single allocation (8 B slot, not Box's 16 B fat
  pointer): `triomphe::ThinArc<Header,[u8]>` (header can cache the u64 hash to
  avoid re-hash on resize; refcounted, copy-on-write writes) OR
  `thin-vec::ThinVec<u8>` (unique ownership, in-place growth). Layout
  `[packed header | (ttl u64) | key_len | key | value]`, key BEFORE value (key is
  immutable), value inlined when header+key+value fits an allocator bin (our
  embstr analogue), else an out-of-line value pointer. Mirrors redis kvobj exactly.
- **Collections** (List/Hash/Set/ZSet, structured) stay boxed structs referenced
  by the entry's value pointer; the entry still collapses the KEY into one
  key+header allocation (the main win).
- **Expected:** 3 allocations/key -> 1 (short strings) or 2 (long/collections),
  an 8 B slot, and NO key duplication -> the redis 8.2 / valkey 8.1 regime
  (~20-30 B/key overhead), closing the small-value gap (today 2.88x at 32B).
- **Risks:** safe bounds-checked blob parsing on every access (keep in one
  property-tested module); ThinArc writes are copy-on-write (rebuild blob, redis
  also reallocs); cache the hash in the header to avoid resize re-hash; embedding
  TTL drops the separate expires index (decide active-expiry: scan or a secondary
  light index); hashbrown's post-doubling trough (~39 B/entry) is the one spot a
  future Dash table would beat - note, do not block.
- This is a large ironcache-store core rewrite (the entry rep + the table +
  the primitives behind the frozen Store waist), staged and gated by the A5
  perf-gate + the full test suite. Sources logged in the research transcript.

### Round 1 detail
Boxed `ValueRepr::{List,Hash,Set,ZSet}` (kvobj.rs) + the rmw dispatch / accessors
(lib.rs); 2 files, ~13 sites, all tests green, sizeof KvObj 112->88, ValueRepr
72->48, slot 128->104. Win was larger than predicted (~105 B/key not ~20) because
the table-bucket-array slack scales with slot size and compounds at the load
factor. Next: the slot is still 104 B; the InlineBuf(45) is now the ValueRepr
bound and the `Option<UnixMillis>`(16) is reserved per key. Round 2 targets those.

## FULL COMPETITOR MATRIX (2026-06-16, all five caches the README names)

After the redis + Dragonfly campaigns, the head-to-head harness was extended to KeyDB
(RESP) and Memcached (non-RESP, memory-only) and the redis/valkey installs were upgraded
to the LEAN current lines (redis 8.x kvobj via packages.redis.io; valkey 8.1.x
embedded-key built from source) so the memory bar is honest, not the apt 7.x stand-in.
All measured on one GitHub `ubuntu-latest` 4-vCPU VM, server pinned to cores 0-1, client
to cores 2-3, 1,000,000 keys, 128B values, YCSB zipf 0.99 90% GET / 10% SET, IronCache 2
shards. IronCache bytes/key 180.27-180.28, qps/core 71996-74115 across the runs.

| Competitor (measured) | bytes/key IC/comp (ratio) | qps/core IC/comp (ratio) | p50 us IC/comp | p99 us IC/comp |
| --- | ---: | ---: | ---: | ---: |
| redis 8.8.0 (kvobj) | 180.28 / 206.16 = **0.87x WIN** | 72903 / 47809 = **1.52x WIN** | 8175 / 7907 | 63679 / 52735 |
| valkey 8.1.8 (embedded key) | 180.28 / 209.55 = **0.86x WIN** | 74115 / 45939 = **1.61x WIN** | 8199 / 8131 | 53471 / 150911 (IC WIN) |
| dragonfly 1.39.0 | 180.27 / 178.60 = **1.01x ~tie** | 72564 / 71549 = **1.01x WIN** | 8119 / 10607 (IC WIN) | 94015 / 108415 (IC WIN) |
| keydb 6.3.4 (dormant) | 180.28 / 240.39 = **0.75x WIN** | 72514 / 59474 = **1.22x WIN** | 9231 / 5951 | 77439 / 25295 |
| memcached 1.6.24 | 180.27 / 194.89 = **0.93x WIN** (memory-only) | n/a (non-RESP) | n/a | n/a |

For reference, the apt 7.x stand-ins (pre-kvobj/embedded-key) were redis 7.0.15 and
valkey 7.2.12, both 232.41 bytes/key (IC 0.78x) and 91789 / 68694 qps/core - IronCache's
edge is LARGER vs 7.x, so the table above uses the leaner 8.x bars to stay honest.

VERDICT: IronCache is PARITY-OR-BETTER on memory against ALL five (wins redis/valkey/
keydb/memcached, ~tie with dragonfly) and FASTER per core than every RESP competitor
(1.01x-1.61x). Latency p50 is ~parity (wins vs dragonfly); p99 is noisy on the shared
runner (wins vs valkey/dragonfly, loses vs redis-8.8/keydb) - not a reliable axis here.
HONESTY: a shared 4-vCPU VM is INDICATIVE; bytes/key is the deterministic/trustworthy
figure; qps/p99 carry runner variance; memcached is memory-only (cross-protocol). The
authoritative bar is still bare metal vs the pinned versions. Surfaced in README.md
"Benchmarks: how it compares". Harness: scripts/bench/headtohead.sh (competitors
redis|valkey|dragonfly|keydb|memcached), .github/workflows/headtohead.yml.
