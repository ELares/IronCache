# ADR-0008: Default eviction policy is S3-FIFO

Status: Accepted
Issue: #46

## Context

With a ceiling and eviction on by default (ADR-0007), IronCache needs one default
online eviction policy, chosen on evidence and reused everywhere. The candidates
from the literature are SIEVE, S3-FIFO, and a W-TinyLFU-fronted FIFO. The policy
must be memory-frugal (bytes-per-key includes per-entry policy metadata) and
lock-light under shared-nothing (ADR-0002), and it must avoid LRU's per-hit
relink, which makes throughput fall at high hit ratio [hit-ratio-can-hurt-throughput].

## Decision

The default policy is **S3-FIFO**, behind the pluggable `EvictionPolicy` trait
(#48) so SIEVE and a W-TinyLFU-fronted variant remain selectable. S3-FIFO
partitions a shard's cache into a small (about 10 percent) probationary FIFO and
a large main FIFO with a ghost queue [s3fifo-small-main-split], using a 2-bit
frequency counter capped at 3 [s3fifo-freq-counter-2bit-cap3]. This exploits that
most objects are one-hit wonders [s3fifo-onehit-wonder-72pct].

## Rejected Alternatives

- **Plain LRU.** Rejected on Efficient: per-hit relinking is a contended
  bottleneck and throughput drops at high hit ratio [hit-ratio-can-hurt-throughput].
- **SIEVE as the default.** Strong and the simplest (one FIFO + a visited bit,
  no ghost) [sieve-simpler-than-lru-nsdi24] [sieve-throughput], kept as a
  selectable policy. Rejected as the default because it has no ghost queue and
  degrades on small caches and scan-heavy block workloads
  [sieve-loc-and-stack-property], whereas S3-FIFO has the best miss ratio on 10
  of 14 datasets [s3fifo-miss-ratio-wins] and about 6x the throughput of an
  optimized LRU at 16 threads [s3fifo-throughput-6x].
- **W-TinyLFU-fronted FIFO as the default.** Best raw hit ratio via its
  windowed admission [wtinylfu-window-main-split], but it carries a frequency
  sketch (more per-cache metadata) than S3-FIFO's 2-bit counter for a marginal
  hit-ratio gain on most traces. Kept as an admission-filter augmentation (#49),
  not the default.

## Consequences

- The default is concurrency-friendly and metadata-frugal, fitting shared-nothing
  and the bytes-per-key target; it lives behind the `EvictionPolicy` trait (#48)
  with a ghost queue.
- The choice is validated empirically on the cachemon corpus plus our own traces
  as the #47 benchmark follow-up; the default is revisited only if that data
  contradicts the published results.
- Redis `maxmemory-policy` names (allkeys-lru, allkeys-lfu, and so on) map onto
  the engine's policies for compatibility in #50; S3-FIFO is the engine default
  under cache mode.
