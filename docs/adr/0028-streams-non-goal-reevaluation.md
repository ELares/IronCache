# ADR-0028: Re-evaluate the Redis Streams non-goal after v1 (stay deferred)

Status: Accepted
Issue: #418

## Context

NON_GOALS entry 12 deferred Redis Streams (XADD, XREAD, XRANGE, consumer groups,
and the backing radix index) for v1 (milestones M0 through M2), and recorded the
deferral as reversible with one piece of guidance: if Streams is ever built, it
should reuse a generic ordered map rather than a bespoke rax. ADR-0009 tiers
Streams in Tier 3 (an extended data type), and ADR-0024 (geo) set the precedent
that a Tier 3 surface gets an explicit in-or-out call rather than a silent
absence. v1 has now shipped (176 commands, the opt-in Raft cluster, persistence,
the verified upgrade), so the deferral is reconsiderable on its own terms rather
than as a v1-scope expedient. This ADR makes that post-v1 call.

Two forces have shifted since the original deferral. First, upstream is still
investing in Streams: recent Redis 8.x releases added ref-counted trimming
(XDELEX, XACKDEL) and stream idempotency and acknowledgement refinements, and
Dragonfly continued to harden its consumer-group replication. So the eventual
cost of a faithful reversal is rising, not static. Second, nothing in v1's
operation produced demand evidence for Streams as a cache surface: Streams is a
durable append-only log with consumer-group bookkeeping, which is a messaging and
event-sourcing primitive more than a caching one, and the project has a sibling
message-bus effort (IronBus) whose mission is exactly that workload.

The tenet order is Compatible greater than Efficient greater than Simple greater
than Scalable greater than AI-Driven. Streams pulls on Compatible (it is a real
Redis surface clients can expect) but pushes hard against Simple: it is a new,
still-growing type with its own command family, trimming and capping semantics,
consumer-group and pending-entries-list state, and blocking XREAD integration,
none of which any existing IronCache type provides. Unlike geo (a scoring veneer
over the existing zset that needs no engine change), Streams is genuinely new
engine surface.

## Decision

Streams stays a **non-goal**, now as a deliberate post-v1 decision rather than a
v1-scope deferral. It remains Tier 3 under ADR-0009 and documented-unsupported,
consistent with the geo treatment in ADR-0024, with three refinements that this
re-evaluation adds:

- The reversal bar is explicit. Streams becomes a candidate to build when there
  is demonstrated user demand for it as a cache surface (not assumed demand), and
  it is then scoped against a generic ordered map per the NON_GOALS entry 12
  guidance, never a bespoke rax. The default in the absence of that demand is to
  keep deferring, because Simple outranks a Compatible gain for a non-core,
  non-cache type.
- If built, the minimum faithful surface is priced in up front so a reversal does
  not immediately reopen a compatibility gap: the core XADD, XLEN, XRANGE,
  XREVRANGE, XREAD, and XDEL plus consumer groups (XGROUP, XREADGROUP, XACK,
  XPENDING, XCLAIM, XAUTOCLAIM), and the newer trimming and idempotency additions
  (XDELEX, XACKDEL) so the build targets the current contract rather than the
  2022 one.
- Streams is recorded as the natural boundary with the sibling message-bus
  project: a durable log with consumer groups is that project's core workload, so
  IronCache deferring Streams is a scope decision, not a capability gap in the
  combined story.

## Rejected Alternatives

- **Build a minimal Streams now.** Ship XADD, XREAD, XRANGE, XLEN plus consumer
  groups on a generic ordered map this milestone. Rejected on Simple and on the
  tenet order: it adds a new, still-evolving type and its consumer-group state
  machine to the engine and the conformance burden for a surface with no
  demonstrated cache demand, exactly the kind of scope creep the ranked tenets
  exist to gate. The newer XDELEX, XACKDEL, and idempotency work also mean a
  minimal build would ship already behind the current contract.
- **Reclassify Streams out of Tier 3 entirely (declare it permanently out).**
  Rejected on Compatible and on honesty: a permanent exclusion overstates the
  decision. The evidence supports deferral with a clear reversal bar, not a
  permanent no, and a hard exclusion would have to be re-litigated the moment
  demand appears.
- **Leave the post-v1 stance implicit.** Rejected on the ADR-0024 precedent: a
  Tier 3 surface with a now-stale v1-era deferral and no post-v1 call leaves the
  published-compatibility contract with a silent hole, which is what the explicit
  in-or-out rule exists to remove.

## Consequences

- The published-compatibility map keeps an explicit, current entry: Streams is
  Tier 3, deferred post-v1, documented-unsupported, with a stated reversal bar.
  NON_GOALS entry 12 is updated to cite this ADR as the post-v1 re-evaluation.
- The conformance suite and differential oracle do not gate on Streams; that
  gating is added only with a future Streams design issue if the reversal bar is
  met.
- Reopening is cheap and non-breaking: a future Streams build is new command
  surface over a generic ordered map with no change to existing types, so it can
  land in a later milestone without superseding any engine decision in this ADR.
  That future issue owns the exact reply-shape and consumer-group contract.
- The decision is recorded as reversible: this ADR is the thing a future demand
  signal reopens, by amending the stance here and filing the design issue, rather
  than by quietly shipping the surface.
