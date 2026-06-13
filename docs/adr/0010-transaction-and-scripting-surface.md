# ADR-0010: Transaction and scripting surface scope

Status: Accepted
Issue: #30

## Context

IronCache is a cache first, not a general transaction engine. Redis ships a deep
transactional and scripting surface (MULTI/EXEC/WATCH plus EVAL and Functions).
Full generality is a large, semantically risky cost that does not help win "most
efficient cache", and an embedded scripting VM on the hot path fights Efficient
and Simple (a committed non-goal, NON_GOALS entry 1). This pins exactly how much
of the surface v1 commits to, and the mechanism.

## Decision

Commit to a **scoped transaction surface on VLL** (the Dragonfly lock-manager
adaptation [dragonfly-vll-citation]), running through the cross-shard coordinator
(#29) that ADR-0002 calls for:

- MULTI / EXEC / DISCARD / WATCH with exact Redis semantics: commands queue at
  MULTI, EXEC applies them, there is **no rollback** on a runtime error, and a
  touched WATCH key aborts EXEC with a null reply [multi-exec-no-rollback].
- The single-shard case is the lock-free fast path (the owning core runs the
  whole transaction); multi-shard transactions hop in txid order through the
  coordinator (#29).
- Lua (EVAL/EVALSHA/SCRIPT) and Functions (FUNCTION/FCALL) are a Tier 4 non-goal
  (ADR-0009); common atomic use cases are served by native atomic ops (#23)
  instead.

## Rejected Alternatives

- **Full Redis transactional + scripting generality (MULTI/EXEC/WATCH + Lua +
  Functions).** Rejected: the scripting VM is a hot-path and Simple cost we do
  not need, and Functions add a library lifecycle surface with no cache payoff.
- **No transactions at all (drop MULTI/EXEC/WATCH).** Rejected on Compatible:
  optimistic-locking MULTI/EXEC/WATCH is part of the contract many clients
  depend on [multi-exec-no-rollback]; dropping it breaks real clients for little
  saving, since the single-shard fast path is cheap.

## Consequences

- Clients using optimistic transactions work; clients depending on server-side
  Lua get a documented unsupported (Tier 4) plus the native atomic-op set (#23).
- The coordinator (#29) implements the txid-ordered multi-shard hop; WATCH
  tracking is shard-local on the owning core.
- No scripting VM is linked into the binary, preserving the no-managed-runtime
  and single-static-binary stance.
