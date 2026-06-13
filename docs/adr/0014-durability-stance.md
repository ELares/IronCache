# ADR-0014: Durability stance (ephemeral default, opt-in snapshot and warm-restart)

Status: Accepted
Issue: #59

## Context

The durability posture must be committed before any persistence work starts,
because it dictates whether durability is a tax on every write or an opt-in
feature. IronCache is a cache first (the Compatible and Efficient tenets), and
the no-fork invariant (invariant 4, NON_GOALS entry 5) already rules out the
Redis BGSAVE model.

## Decision

**Ephemeral by default.** No durability cost on the default hot path. Above that,
an opt-in menu:

1. **Forkless versioned point-in-time snapshot** (#60), with constant extra
   memory and no `fork()` [dragonfly-forkless-versioned-snapshot].
2. **mmap warm-restart** (#62): a graceful shutdown writes a state file and a
   restart rebuilds in seconds [memcached-warm-restart-mmap-sigusr1], so a
   planned restart skips the cold-cache penalty.
3. **Tiered SSD and an append-log** as later tiers (#63, #66), not v1 defaults.

This matches Garnet's safe-by-default posture (storage tiering and AOF both off
by default) [garnet-storage-tier-default-off] [garnet-aof-default-off].

## Rejected Alternatives

- **Durable by default (snapshot or append-log on out of the box).** Rejected on
  Efficient and Simple: it taxes every write and complicates the zero-config
  story for a workload (caching) that is usually reconstructable from the source
  of truth.
- **No durability at all, ever (purely ephemeral, no opt-in).** Rejected: warm
  restart and point-in-time snapshots are real operational wins for cache
  warming and backup; refusing them entirely is needlessly limiting. The contract
  stays a cache contract (NON_GOALS entries 5, 8), not an ACID one.

## Consequences

- The default binary has no persistence code on the hot path; durability is
  additive and opt-in.
- #60 (forkless snapshot), #62 (warm restart), #63 (append-log), and #66 (SSD
  tier) are the opt-in tiers built on this stance; the snapshot-overhead bound is
  the #61 research.
- Because the default is ephemeral, the consistency non-goal (NON_GOALS entry 8)
  and the no-claim-without-test rule (entry 9) govern what durability language we
  may advertise per tier.
