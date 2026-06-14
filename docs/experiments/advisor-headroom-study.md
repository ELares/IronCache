# Experiment: Advisor headroom over a tuned W-TinyLFU + SIEVE floor (one-time go/no-go)

Issue: #90. Provisional decision: ADR-0013 ships the advisor off/shadow by default and names this headroom study as the evidence that justifies turning it on at all; ADR-0016 fixes the metrics the verdict is reported in.

This is a one-time go/no-go that gates the advisor engineering spend in ADVISOR.md (#126) and its parent (#88). It does not design the advisor and it does not re-decide ADR-0013. It defines the replay, the tuned baseline the advisor must beat, the marginal-hit-ratio threshold that justifies the build, and what would flip the verdict.

## Provisional decision (already pinned)

ADR-0013 (Accepted, issue #155) ships the advisor off by default and shadow when first enabled, and explicitly names the #90 headroom study as the safe way to gather the evidence that justifies active tuning, before any live policy is touched. ADR-0016 (Accepted, issue #7) fixes the metrics: the verdict is reported as marginal hit ratio at matched cache sizes, and any throughput or memory side effect of a candidate advisor is judged on the per-core-throughput and bytes-per-key headline pair. ADVISOR.md (#126) already pins the mechanism the study evaluates: a regret-minimizing controller weighting cheap deterministic experts {SIEVE, W-TinyLFU-admission, sampled LRU/LFU} off the hot path, with S3-FIFO as the baseline eviction core (ADR-0008). This doc records the procedure that decides whether that mechanism earns its keep. It does not pre-judge the answer.

The provisional bar this study proposes and defends: build the advisor only if the median KV-trace marginal hit-ratio gain over the per-trace-tuned floor clears a fixed threshold at the cache-to-working-set ratios IronCache actually runs. The threshold is proposed here as a decision parameter, not as a measured result. It is defended below against the operational cost of an adaptive component, not asserted as a number the study already produced.

## Why this is harness-blocked

The go/no-go needs replayed marginal hit ratio against a tuned floor, which requires three things that do not exist yet:

- The trace-replay path and ADR-0016 measurement methodology in the #8 harness, sharing the eviction-trait fixtures used by the bake-off (#47).
- A per-trace tuning pass for the deterministic floor (W-TinyLFU window/sketch and SIEVE parameters), so the advisor competes against IronCache's best deterministic effort and not a strawman. A poorly tuned floor would overstate any advisor gain, which is the central measurement hazard for this study.
- The Belady-MIN ceiling and per-policy gap from EVICTION_ORACLE.md (#93), so the marginal gain has a denominator: the gain is reported as a fraction of the headroom the floor actually leaves on the table, not as a bare hit-ratio delta.

Until the harness can replay the KV-weighted corpus against a per-trace-tuned floor and report the gap to the #93 ceiling, any advisor-versus-floor number is a citation comparison across non-KV home corpora, which is exactly the generalization hazard #90 flags: the published adaptive-advisor gains were earned on CDN, block, and flash traces, not in-memory KV.

## Experiment to run

Corpus and weighting:

- Replay Twitter Cluster, Meta/CacheLib, MSR, and Wikipedia. Weight the verdict toward in-memory KV traces (Twitter, Meta) so a positive result generalizes to IronCache deployments rather than to CDN or block storage. MSR and Wikipedia are run for breadth and scan/churn coverage but do not carry the median KV verdict.
- Sweep cache size so the cache-to-working-set ratio spans from small (where adaptive advisors win most) to the large, frequency-dominated sizes IronCache expects to run.

The tuned floor (the bar to beat):

- Per trace, tune the W-TinyLFU admission filter (window size and the 4-bit count-min sketch aging) [wtinylfu-caffeine-sketch] and SIEVE (its single FIFO, hand, and visited-bit mechanics) [sieve-algorithm] before measuring. The floor is tuned per trace, the advisor is not given that per-trace oracle, so the comparison is honest about what the advisor must discover online.
- Report the tuned floor's hit ratio and its gap to the Belady-MIN ceiling (#93) at each size point, so the advisor's room to improve is a measured headroom, not an assumption.

The candidate advisor:

- A LeCaR/CACHEUS-style regret-minimizing controller weighting the deterministic experts, matching the ADVISOR.md mechanism [lecar-regret-min-18x][lecar-regret-minimization-smallcache][cacheus-experts-fast21][cacheus-experts]. Treat the published gains as an upper bound earned on non-KV traces, not a forecast for KV.
- Measure marginal hit ratio at matched cache sizes (advisor minus tuned floor), the advisor's per-request cost against the throughput budget, and any throughput-per-core or bytes-per-key side effect under ADR-0016 methodology.

The decision rule and its defended threshold:

- Build only if the median KV-trace (Twitter, Meta) marginal gain over the tuned floor clears the threshold at the cache-to-working-set ratios IronCache actually runs, AND the advisor's per-request cost stays within the throughput budget, AND the advisor does not regress throughput-per-core or bytes-per-key on the headline pair.
- The threshold is defended as follows. The advisor adds operational cost: a per-shard background loop, a safety envelope, an audit log, and the mis-tuning risk that a regret-minimizer which helps tiny caches can underperform on large, frequency-dominated caches [lecar-regret-minimization-smallcache]. Per the tenets, Simple beats AI-Driven when the marginal gain is thin. So the threshold is set high enough that the median KV gain must visibly exceed the floor's own run-to-run noise band and a meaningful fraction of the floor-to-Belady headroom (#93), not merely be positive. A gain that is real but smaller than the headroom the floor already leaves uncaptured does not clear the bar, because tightening the deterministic floor would be the simpler win. The exact numeric threshold is calibrated once the floor's noise band and the per-trace headroom are measured; it is not invented here.

## What would change the decision

- Go: the median KV-trace marginal gain over the tuned floor clears the threshold at IronCache's real cache-to-working-set ratios and survives charging the advisor's per-request cost against the throughput budget.
- No-go (the default expectation per ADR-0013's posture): the median KV gain is below threshold, or it concentrates only at small cache sizes and evaporates at the large, frequency-dominated sizes IronCache runs [lecar-regret-minimization-smallcache], or the advisor's per-request cost breaches the throughput budget [cacheus-experts].
- The verdict flips if the tuned floor turns out under-tuned: a stronger per-trace W-TinyLFU + SIEVE tuning [wtinylfu-caffeine-sketch][sieve-algorithm] that closes more of the #93 headroom would shrink the advisor's marginal gain and could move a borderline go to a no-go.
- The verdict also flips if the #93 ceiling shows the floor already captures most available headroom on KV traces, in which case the advisor's room to improve is small by construction regardless of its mechanism.

## References

- ADR-0013: advisor off/shadow default posture, which names this study as its justifying evidence (issue #155). ADR-0016: headline metrics and methodology (issue #7).
- ADVISOR.md (#126): the advisor mechanism this study gates. EVICTION_ORACLE.md (#93): the Belady-MIN ceiling and per-policy gap that denominate the marginal gain. #89: the objective metric the advisor optimizes. #87: the online gap estimator.
- #88: parent advisor design. #47: eviction bake-off (shared fixtures and tuned-floor framing). #8: benchmark harness. #1: vision.
- Claims (resolved via docs/prior-art/claims.yaml): [lecar-regret-min-18x], [lecar-regret-minimization-smallcache], [cacheus-experts-fast21], [cacheus-experts], [wtinylfu-caffeine-sketch], [sieve-algorithm].
