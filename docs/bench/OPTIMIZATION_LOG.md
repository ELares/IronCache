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

### Round 1 detail
Boxed `ValueRepr::{List,Hash,Set,ZSet}` (kvobj.rs) + the rmw dispatch / accessors
(lib.rs); 2 files, ~13 sites, all tests green, sizeof KvObj 112->88, ValueRepr
72->48, slot 128->104. Win was larger than predicted (~105 B/key not ~20) because
the table-bucket-array slack scales with slot size and compounds at the load
factor. Next: the slot is still 104 B; the InlineBuf(45) is now the ValueRepr
bound and the `Option<UnixMillis>`(16) is reserved per key. Round 2 targets those.
