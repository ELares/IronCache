# ADR-0015: Default value codec is zstd (low level), with LZ4 and none as policy options

Status: Accepted
Issue: #53

## Context

Transparent server-side value compression is a headline memory win Redis
structurally refuses to provide: it compresses only RDB snapshots with LZF and
never in-memory values [redis-rdbcompression-default-yes-lzf]
[redis-no-transparent-value-compression]. Once IronCache commits to compressing
values (#52), the first decision is which codec sits in the default path. The
choice trades CPU on the hot path against bytes-per-key (the Efficient tenet).

## Decision

The default value codec is **zstd at a low (including negative) level**, with
**LZ4** and **none** exposed as per-keyspace runtime policy. zstd leads the
ratio frontier (zstd 1.5.7 level 1 reaches 2.896 on Silesia vs LZ4 2.101 and
Snappy 2.089) [zstd-silesia-benchmark-l1] [lz4-silesia-benchmark]
[snappy-lzf-silesia-benchmark], and its negative/fast levels trade ratio for
speed when CPU is tight [zstd-fast-modes-benchmark]. Trained dictionaries for
small similar values (#55) are a zstd strength (1 KB records went from 2.8x to
6.9x with a dictionary) [zstd-dictionary-small-data-6.9x]. Compression is gated
by value size and entropy (#52/#92), so it never runs where it cannot pay.

## Rejected Alternatives

- **LZ4 as the default.** Faster and pure-Rust via lz4_flex [lz4-flex-safe-vs-c],
  kept as the speed-first policy option, but its ratio (2.101) leaves memory on
  the table versus zstd, and memory-per-key is the headline metric. Operators
  who want minimal CPU select LZ4 per keyspace.
- **No compression (none) as the default.** That is just Redis's status quo
  [redis-no-transparent-value-compression]; it forfeits the memory win that is a
  core differentiator. `none` stays available per keyspace for
  incompressible/latency-critical data.
- **Snappy / LZF.** Rejected: both trail zstd and LZ4 on the same frontier
  [snappy-lzf-silesia-benchmark] with no offsetting advantage for IronCache.

## Consequences

- The C-bound-vs-pure-Rust zstd question for the static binary is decided
  separately (#54); the codec choice here is logical, the linkage is #54.
- Per-keyspace codec policy (zstd / lz4 / none) is a config surface (#85), with
  zstd-low the default.
- Compression interacts with mutating commands and hot keys (#56) and with the
  off-path compression-decision model (#92); the size/entropy gate keeps it off
  the path where it does not pay.
