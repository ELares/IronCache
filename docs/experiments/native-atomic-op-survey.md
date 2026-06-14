# Experiment: Native atomic-op survey (which native verbs displace real EVAL traffic)

Issue: #23. Provisional decision: ADR-0010 (transaction and scripting surface) and docs/research/resp-protocol-compat.md (area protocol) pin Lua as a Tier-4 non-goal and route common atomic use cases to a native atomic-op set instead.

## Provisional decision (already pinned)

ADR-0010 (Accepted, issue #30) commits to a scoped transaction surface (MULTI / EXEC / DISCARD / WATCH with exact Redis semantics, no rollback, WATCH-abort to null [multi-exec-no-rollback]) and declares EVAL / EVALSHA / SCRIPT and Functions a Tier-4 non-goal (ADR-0009): no scripting VM is linked into the binary, and common atomic use cases are served by native atomic ops (#23) instead. The reason scripting exists is atomicity across a read-modify-write, not general computation; MULTI/EXEC already gives a queue-then-apply boundary but costs a WATCH round trip plus an EXEC round trip and aborts under contention [multi-exec-no-rollback], whereas a single native verb collapses the read and the write into one server-side step. Functions are the persisted successor to ephemeral EVAL scripts and both block the whole server during execution [functions-redis-7.0]; ADR-0010 rejects that whole surface.

This doc does not re-decide the non-goal. It records the survey that validates the trigger: ADR-0010's premise is only defensible if the workloads people reach for Lua to solve have a native answer. The provisional native set to validate (carried from the issue, not chosen here):

- Compare-and-set. The Redis 7.0 `SET key val IFEQ expected` idiom as a first-class CAS rather than a new verb; reply `+OK` or null (`$-1` RESP2, `_` RESP3) on mismatch [resp2-null-encodings].
- Counter with TTL. `INCR` / `INCRBY` exist; the gap is increment-and-expire-on-create atomicity, candidate verb `INCREX key incr ttl` (RESP integer reply).
- Fixed-window throttle. Candidate verb `CLTHROTTLE key limit window`: atomically increment, set TTL to `window` on first hit, reply a multi-bulk array `[allowed, remaining, retry_after]` [resp-type-prefixes].

The candidate verbs `INCREX` and `CLTHROTTLE` and the `IFEQ` option carry no claim id; they are IronCache design proposals, not facts about a prior-art system.

## Why this is harness-blocked

The decision rule needs a measured displaceable fraction over real scripting traffic, which requires three things that do not exist yet:

- A representative EVAL / EVALSHA corpus. The fraction of scripting traffic that the native set covers is an empirical property of a workload, not of the spec; no IronCache corpus of real scripts and their call frequencies has been captured.
- A classifier that maps each sampled script to one of the canonical patterns (check-and-set, atomic-counter-with-cap, fixed-window rate limit) or to an unclassified tail, with the tail being exactly the signal that genuine programmability is being asked for.
- Working native verbs behind the per-shard execution boundary that ADR-0010 already requires for transactions, so a script and its proposed native replacement can be run for behavioral equivalence rather than judged by inspection.

Until a corpus is replayed and classified under one model, any claim about how much traffic the native set displaces is an assertion, which is what this experiment exists to replace with a number.

## Experiment to run

Corpus and workload:

- A sampled EVAL / EVALSHA corpus: distinct script bodies plus their observed call frequency and KEYS/ARGV arity, drawn from public Redis script collections and from any captured IronCache-target traffic. Frequency-weighting matters because the decision is about traffic displaced, not distinct scripts.
- A labeled fixture set of the three canonical patterns (check-and-set, atomic-counter-with-cap, fixed-window token bucket) used to validate the classifier before it runs on the wild corpus.
- An adversarial tail set: scripts with loops, cross-key fan-out, or arbitrary control flow, included so the unclassified fraction is measured rather than assumed to be small.

Fixed parameters (held identical across the run):

- The canonical pattern taxonomy: check-and-set, atomic-counter-with-cap, fixed-window throttle, and a single unclassified bucket. No pattern is added mid-run to absorb the tail.
- The candidate native set: `SET ... IFEQ`, `INCREX`, `CLTHROTTLE`, plus the existing `INCR` / `INCRBY` / `GETEX` family.
- Behavioral-equivalence checking against the per-shard execution boundary (single-shard lock-free fast path per ADR-0010), so equivalence is judged by replies, not by source similarity.
- RESP reply shaping decided in #17 / ADR-0019, including RESP2-versus-RESP3 null and aggregate encodings [resp2-null-encodings] [resp-type-prefixes].

Varied parameters:

- Corpus mix: public collections only, versus public plus captured traffic, to expose how sensitive the displaceable fraction is to source.
- Classification strictness: exact-pattern match versus match-after-trivial-rewrite (for example a script that only renames KEYS), to bound the fraction.
- Composition policy: throttle served by a bespoke `CLTHROTTLE` verb versus throttle composed client-side from `INCREX`, to weigh one round trip against command-count growth near the ~240 core budget [redis-core-command-count].

Measured:

- The frequency-weighted fraction of sampled scripting traffic that each canonical pattern covers, and the combined coverage of the native set.
- The size and composition of the unclassified tail, frequency-weighted.
- For each covered script, whether the native verb is round-trip behavioral-equivalent (same reply across RESP2 and RESP3) to the original script.
- The marginal command-count cost of each bespoke verb against the ~240 core budget [redis-core-command-count].

Decision rule:

- Keep Lua out (ADR-0010 trigger NOT met) if the native set covers the bulk of frequency-weighted traffic AND the unclassified tail contains no high-frequency script that needs atomicity the primitives cannot express.
- Promote `CLTHROTTLE` to a bespoke verb only if fixed-window throttle is a high-frequency pattern AND composing it client-side from `INCREX` costs an extra round trip that the throttle hot path cannot absorb; otherwise compose it and hold the command count near ~240 [redis-core-command-count].
- Revisit the Tier-4 non-goal (the only path to Lua) only if a high-frequency tail script needs arbitrary control flow, loops, or cross-key fan-out that no native verb can express.

## What would change the decision

- The unclassified, frequency-weighted tail is large, or contains a high-frequency script needing atomicity beyond CAS plus counter-with-TTL plus fixed-window throttle, which is the documented signal to re-open the Tier-4 non-goal.
- A captured-traffic corpus shifts the displaceable fraction materially below the public-collection result, showing the survey was source-biased.
- A candidate native verb cannot be made round-trip behavioral-equivalent to the scripts it claims to replace under the per-shard boundary, so the native answer is incomplete.
- `CLTHROTTLE` fails to earn its slot: throttle is rare, or composes acceptably from `INCREX` without a hot-path round-trip penalty, so the bespoke verb is dropped to protect the command budget [redis-core-command-count].

## References

- ADR-0010: transaction and scripting surface, Lua Tier-4 non-goal, native atomic ops (issue #30). ADR-0009: compatibility tiering. docs/research/resp-protocol-compat.md: protocol contract. #23: this survey; parent #15; vision #1; related #10, #30.
- Claims (resolved via docs/prior-art/claims.yaml): [functions-redis-7.0], [multi-exec-no-rollback], [redis-core-command-count], [resp2-null-encodings], [resp-type-prefixes], [bulk-string-max-512mb], [client-default-resp3-redis8], [resp3-opt-in-via-hello].
