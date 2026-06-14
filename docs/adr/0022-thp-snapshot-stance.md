# ADR-0022: THP and snapshot stance (forkless serialization, no huge pages, no overcommit tuning)

Status: Accepted
Issue: #44

## Context

IronCache persists and replicates without `fork()`. The historical reason a cache
cares about Transparent Huge Pages (THP) is the copy-on-write (COW) fault storm a
forking snapshot triggers: with THP enabled, a write after `fork()` copies a 2 MiB
huge page instead of a 4 KiB base page, multiplying the latency and memory blowup
during a save, which is why Redis recommends disabling THP
[redis-thp-cow-blowup], and under write load the forking child's COW can approach
2x process RSS [redis-cow-rss-doubling]. The Redis fork model also pushes
`vm.overcommit_memory` kernel tuning onto operators to keep the save from failing
[redis-vm-overcommit-memory].

This ADR resolves only the page-size-vs-snapshot interaction and the operator
tuning surface. It does not reopen the durability posture or the snapshot
mechanism: the ephemeral default and the forkless versioned snapshot are owned by
[ADR-0014](0014-durability-stance.md) (#59), and the global allocator is owned by
[ADR-0006](0006-default-allocator-and-accounting.md) (#41). This record settles
what those decisions imply for huge pages and overcommit. The tradeoff resolves
under our ranked tenets (Compatible > Efficient > Simple > Scalable > AI-Driven).

## Decision

- **Snapshots are forkless and serialized.** Because the snapshot is taken by
  serializing a consistent view through an epoch cut rather than by `fork()`
  [dragonfly-forkless-snapshot-mechanism] (the mechanism is ADR-0014's #60), no
  COW faults occur, so page size cannot harm persistence through the COW
  mechanism at all. This ADR does not re-decide that the snapshot is forkless; it
  records the consequence: with no fork, the entire THP-versus-COW hazard is gone.
- **The heap requests no huge pages.** IronCache calls
  `madvise(MADV_NOHUGEPAGE)` on the allocator arenas (the arenas owned by
  ADR-0006), mirroring the jemalloc default of not backing arenas with huge pages
  [jemalloc-thp-default]. The TLB win from huge pages on pointer-chasing cache
  access did not justify the operational surface; the heap stays on base pages.
- **THP=always is warning-only.** When the host runs THP=always, IronCache emits
  one informational startup line mirroring Redis guidance
  [redis-thp-cow-blowup], then runs normally. Because IronCache does not fork, a
  misconfigured THP setting degrades nothing user-visible, so the warning is
  informational, never a hard requirement or a startup refusal.
- **No `vm.overcommit_memory` tuning is a supported prerequisite.** IronCache does
  not require operators to set `vm.overcommit_memory`, disable THP, or otherwise
  tune the kernel to get correct persistence [redis-vm-overcommit-memory]. The
  product runs correctly on a stock host.

## Rejected Alternatives

- **`fork()` + COW snapshot (the Redis model).** A child sees a frozen heap for
  free, but write-heavy workloads dirty COW pages and can push RSS toward 2x
  used_memory [redis-cow-rss-doubling]; THP=always then copies 2 MiB per fault,
  multiplying that blowup [redis-thp-cow-blowup], and it forces overcommit tuning
  on operators [redis-vm-overcommit-memory]. Rejected: it violates the no-fork
  stance ADR-0014 already committed, and it is the sole reason huge pages would be
  dangerous here.
- **A hugepage-backed heap (THP=always or explicit 2 MiB arenas).** Fewer TLB
  misses on large scans, but on our hash and skiplist access microbenchmarks the
  TLB win was within noise; under any fork path it would amplify COW cost
  [redis-thp-cow-blowup], and it adds allocator complexity the tenets do not
  justify for a cache. Rejected: Simple and Compatible outrank the marginal
  Efficient gain. The choice not to fork is what makes a hugepage heap pointless
  rather than merely dangerous.
- **Requiring operators to disable THP and set overcommit at install.** Rejected:
  it is the very operational tax the forkless design exists to remove
  [redis-vm-overcommit-memory]; the stock-host default is part of the
  zero-config promise.

## Consequences

- Page size is irrelevant to persistence correctness: the forkless serializer
  [dragonfly-forkless-snapshot-mechanism] never triggers COW, so neither THP nor
  overcommit can break a save.
- The heap runs on base pages by `MADV_NOHUGEPAGE` on ADR-0006's arenas, matching
  jemalloc's own no-huge-pages-by-default posture [jemalloc-thp-default]; the
  arena and accounting specifics remain owned by ADR-0006.
- A THP=always host produces one warning line and otherwise behaves identically,
  so IronCache stays correct on stock and on tuned hosts without operator action.
- This ADR is revisitable only if a future benchmark shows a real per-core win
  from a hugepage heap on cache access; until then base pages stand. The snapshot
  serialization and durability menu it rests on are specified in PERSISTENCE.md
  (#58).
