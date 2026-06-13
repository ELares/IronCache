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
| #16 compatibility tiering (Tier 0-4) | protocol | none | Impl. Readiness | yes |
| #30 transaction + scripting surface | protocol | none | Impl. Readiness | no |
| #59 durability stance | persistence | #61 snapshot-overhead | Impl. Readiness | yes |
| #53 default codec | compression | #57 value-size survey | Impl. Readiness | no |
| #69 single-node-first + slot-ready | replication | none | Impl. Readiness | yes |
| #7 headline metrics | performance | #9 single-core bar | Impl. Readiness | yes |
| #157 per-tenet acceptance gates | governance | #7, #16, #24 | Impl. Readiness | no |
| #155 advisor default posture | ai | none | Impl. Readiness | no |
| #146 headline scale-out targets | replication | clustering (Wave 3) | M0 | no |
| #37 adaptive vs fixed encoding thresholds | datastructures | #57 | M0 | no |
| #33/#36 dependents (allocator cross-shard frees) | memory | #42 | M1 | no |

An entry is removed from this register the moment its ADR is accepted and listed
in [INDEX.md](INDEX.md).
