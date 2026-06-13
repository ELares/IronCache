# Committed non-goals

IronCache wins as much by what it refuses to build as by what it builds. This
register lists the surfaces IronCache will NOT ship in v1 (milestones M0 through
M2), each traced to the tenet or deferred milestone that justifies it. Every
entry is a reversible scope decision under the charter (#2) and the vision EPIC
(#1): reversing one means reopening its issue and amending here, never quietly
shipping the surface. Tenet order: Compatible > Efficient > Simple > Scalable >
AI-Driven.

## Protocol and runtime surface (#10)

1. **Embedded Lua scripting and Redis Functions.** No `EVAL`/`EVALSHA`/`SCRIPT`
   and no `FUNCTION`/`FCALL`. A scripting VM on the hot path fights Efficient and
   Simple; common atomic use cases are served by native atomic ops (#23). Tier 4
   in the compatibility tiering (#16).
2. **Memcached protocols and emulated cluster mode.** No Memcached text or meta
   protocol; the Redis wire protocol is the one contract (Compatible). We do not
   copy the single-process emulated cluster stopgap; multi-node is a real design
   (Scalable, #68).
3. **RDMA and other exotic transports.** TCP (and Unix sockets) only in v1; RDMA
   is deferred, not designed in (Simple).
4. **A managed runtime.** No JVM, no .NET, no GC. The shipping artifact is one
   static native binary with no runtime dependency; Garnet's strong numbers come
   on .NET 8.0 [garnet-bench-baselines], a dependency we refuse on Simple grounds.

## Persistence and host posture (#11, #101, #102, #103)

5. **No `fork()`+copy-on-write snapshotting** (the Redis BGSAVE model). Fork+COW
   doubles RSS under write load [redis-cow-rss-doubling] and stalls proportional
   to heap size [redis-fork-latency-per-gb]. Point-in-time durability comes
   solely from the forkless versioned snapshot [dragonfly-forkless-versioned-snapshot]
   (#60); no second snapshot path will be added. (Efficient; invariant 4.)
6. **No host kernel-tuning preconditions.** IronCache runs correctly on stock
   settings; it will not require disabling Transparent Huge Pages (THP amplifies
   COW to 2 MB pages [redis-thp-cow-blowup]) or setting `vm.overcommit_memory=1`,
   both of which are artifacts of the fork model we reject. (Simple.)
7. **No mandatory hot-path proxy.** No required twemproxy/Envoy/dynomite hop.
   Routing is smart-client MOVED/ASK redirection [redis-cluster-moved-ask] (#70);
   an embedded router may ship later, opt-in and off the default path.
   (Efficient, Simple.)

## Consistency (#12, #14)

8. **No strong consistency in the default mode.** The default replication tier
   is asynchronous leader-follower, so an acknowledged write can be lost on
   failover, exactly as Redis WAIT is not a strong-consistency guarantee
   [redis-wait-not-strongly-consistent]. We will not expose a WAIT-style
   acknowledgement in the default tier that can be read as a consistency
   guarantee. Strong durability is an explicit opt-in quorum tier, never the
   default. Promising Redis-async semantics as strong consistency would be a
   silent behavioral divergence, the worst kind of incompatibility. (Compatible
   first, then Efficient and Simple.)
9. **No consistency or efficiency claim without its test.** We will not advertise
   a consistency level for any mode that has not passed a Jepsen suite checked by
   Elle (Redis-Raft itself shipped with 21 such issues [jepsen-redis-raft-21-issues]),
   and no headline efficiency number ships without its reproducible benchmark.
   This is a governance rule, enforced from M0. (Compatible, Efficient.)

## AI on the data path (#13, #156)

10. **No per-request ML inference on the hot path.** Eviction, admission, and
    lookup stay in hand-written, branch-predictable code; a forward pass per
    access is incompatible with a nanosecond-scale `GET`/`SET`. Learned-Belady
    and Parrot-class policies [parrot-imitation-belady-icml20] and online ML
    ensembles [lecar-regret-minimization-smallcache] are confined to the off-path
    advisor. (Efficient over AI-Driven.)
11. **No runtime AI dependency.** The running engine makes zero calls to an
    external model service or cloud LLM, links no GPU or heavy-ML runtime, and
    contains only a cheap in-process O(1) controller. LLM/agent work is the
    development method (this repository), not a runtime feature. (Simple over
    AI-Driven; preserves the single-static-binary contract.)

## Data types (#132)

12. **No Redis Streams in v1.** `XADD`/`XREAD`/`XRANGE`, consumer groups, and the
    backing radix index are out of scope for M0 through M2. Recording the absence
    keeps the compatibility surface honest (#16 parks Streams in Tier 3); if ever
    built, they would reuse a generic ordered map, not a bespoke rax. (Compatible
    honesty; deferred.)

Each numbered entry corresponds to a closed `[NON-GOAL]` issue. Observability
note: IronCache will, unlike Redis [redis-no-builtin-prometheus], expose native
metrics; that is a goal, recorded here only to contrast with the runtime
non-goals above.
