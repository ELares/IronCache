# Open-decision register

Decisions not yet frozen into an ADR. Each lists the owning area, the research
that must land first, the target milestone, and whether it is critical path in
the sense of issue #4 rule 4: a decision that GATES other decisions. This is a
broader set than the thin-slice `critical-path` label (which marks the issues on
the first running binary, see the [roadmap](../ROADMAP.md)); a gating decision
need not itself be on that slice. The concurrency model (#24) is the root: it
gates the per-shard map (#36), the allocator (#41), and the persistence/fork
stance.

| Decision issue | Area | Blocked on (research) | Milestone | Critical path |
| --- | --- | --- | --- | --- |
| #33/#36 dependents (allocator cross-shard frees) | memory | #42 | M1 | no |

An entry is removed from this register the moment its ADR is accepted and listed
in [INDEX.md](INDEX.md).
