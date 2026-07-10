# IronCache Production Readiness Report

Synthesis of 8 dimension audits plus adversarial verification of every P0/P1 finding. All claims below are evidence-cited (file:line or issue/PR number); refuted findings are excluded from the gap list and recorded in section 5.

---

## 1. Executive Verdict: READY (single-node / authenticated); cluster hands-off ops still maturing

This is the post-verification refresh (each status re-checked against the code + git history; see section 4). Every P1 the original audit confirmed has since shipped. The two pre-auth/pre-cap memory-exhaustion vectors are capped (query buffer at serve.rs:2108-2114, intra-batch output enforced inside the decode loop at serve.rs:1714-1723, PR #542), the silent-empty-boot on a format-version mismatch is now a loud `UnknownVersion` failure (format.rs:233, PR #540), INFO is aggregated node-wide so it matches /metrics and DBSIZE (dispatch.rs:383, PR #545), a command-latency histogram is exported (PR #547), RLIMIT_NOFILE is budgeted at boot (PR #539), the merge-gate deadlock is fixed under cargo-nextest (PR #537), the parser fuzz gate ships (PR #541), and the actively-false CONTRIBUTING.md is rewritten (PR #538). On top of the P1s the operator surface is now real: honest INFO/repl metrics with a true ops/sec and rdb_last_bgsave_status (PR #554), a Grafana dashboard + alert rules + METRICS.md with metrics default-on (PR #558), an operator RUNBOOK (PR #552), symbolized crashes (PR #553), a first `v0.1.0` release with a pinned upgrade key (PR #566/#567), hot TLS cert reload on SIGHUP (PR #584), and unauthenticated-cluster-bus / unknown-env-var warnings (PR #560). A single-node, authenticated deployment supervised by systemd or Kubernetes is now defensible for monitoring-dependent fleets too, backed by the same strong correctness suite (deterministic-simulation Raft verification, a merge-gating differential test against redis-server, a 3-client driver matrix) and supply-chain discipline (committed Cargo.lock, cargo-deny, SHA-pinned actions, SBOM, minisign plus Sigstore) the original audit verified. What remains is NOT a P1: hands-off CLUSTER operations are still maturing (the live rolling-upgrade driver and its end-to-end proof are unlanded, #392/#391 OPEN), there is no mixed-version compatibility CI, the differential oracle is unpinned and coverage is unmeasured, and a handful of P3 polish items (god-file splits, property tests, overflow-checks, run_id/redis_mode/MONITOR honesty) are open. No P0 was ever confirmed; client-port mTLS remains out of scope.

---

## 2. Scorecard

Re-scored after verification (see section 4). Scores rise where the blocking items are now shipped; residual gaps keep each honest.

| Dimension | Score /10 (was) | Status |
|---|---|---|
| Test coverage & correctness assurance | 8.0 (6.5) | Fuzz gate now ships (PR #541) and the load-dependent merge-gate deadlock is fixed under cargo-nextest (PR #537); remaining gaps are no coverage measurement + an unpinned differential oracle (item 19), no property tests (item 25), and no mixed-version compat CI (item 11) |
| Security & public trust | 9.0 (7.5) | Query-buffer cap (PR #542), parser fuzzing (PR #541), hot TLS cert reload (PR #584), the unauthenticated-cluster-bus warning (PR #560), and the console web-surface hardening (#369, PR #607: no open redirect, link-local/metadata egress block, nosniff/no-store, CSRF gate, auth rate limit, adversarial-review-clean) all landed; overflow-checks-off in release is the residual |
| Monitoring & observability | 9.0 (6.5) | INFO aggregation (PR #545), a command-latency histogram (PR #547), honest repl/ops metrics (PR #554), and a shipped dashboard + alerts + METRICS.md default-on (PR #558) close both headline gaps; residuals are total_net_input/output_bytes, the run_id placeholder, and the redis_mode string |
| Troubleshooting & operability | 8.5 (6.5) | Node-wide INFO, symbolized crashes (PR #553), an operator RUNBOOK (PR #552), and env-var warnings (PR #560); MONITOR is intentionally unimplemented (returns "MONITOR is not supported"), so the residual is minor INFO honesty polish (run_id placeholder, hardwired redis_mode) |
| Upgrade & lifecycle safety | 7.5 (6.0) | Format-version loud (PR #540), a first v0.x release (PR #566), documented + logged socket activation (PR #592), a rolling-upgrade runbook (PR #565), and the #390 tmpfs handoff all shipped; the live cluster rolling-upgrade driver/E2E (#392/#391) and mixed-version compat CI remain |
| Codebase maintainability & contributor scalability | 8.5 (7.5) | CONTRIBUTING.md rewritten for the implementation phase (PR #538); the three god files and absent property tests are the remaining drag |
| Service scalability & resource safety | 8.5 (n/s) | Both confirmed P1s closed: RLIMIT_NOFILE budgeting at boot (PR #539) and intra-batch output-buffer enforcement (PR #542) |
| Goal completion audit | 8.5 (n/s) | Issue-resolution largely done and the #517 hop-elimination chain is complete (#525 merged + driver-matrix cluster leg #569); the competitive goal is MET on GET: the corrected thread-per-core re-bench (c7g, 2026-07-10) shows IronCache leading GET by about 19% single-endpoint (2.43M vs Dragonfly 2.04M) and by roughly 2x cluster-aware (4.32M vs 2.19M via #517 zero-hop), so #507 is CLOSED. Honest nuance: the baseline tail TIES Dragonfly (~15ms) and the durable-save tail (291ms) is competitive, not category-leading |

The two dimensions previously marked n/s are now scored: their confirmed P1s (RLIMIT_NOFILE, intra-batch output cap) are resolved and the goal-completion state is re-verified below.

---

## 3. Strengths to Market

What the public can genuinely trust today, each independently verified:

1. **Deterministic-simulation-tested Raft.** Byte-identical seeded replay (crates/ironcache-sim/src/lib.rs:34-53) drives figure-8 commit safety (ironcache-raft/src/lib.rs:5820), partition/heal, election safety under drops, and membership change under partition, plus end-to-end 3-node tests over real sockets covering live slot migration, replica full-sync, min-replicas-to-write, and learner auto-promote (crates/ironcache/tests/raft_cluster.rs:248-1912).
2. **Merge-gating differential testing against real redis-server.** A 205-step corpus (strings through HLL, WRONGTYPE matrix, MULTI/EXEC/WATCH, binary-safe keys) with a principled normalization policy runs in CI on every merge (.github/workflows/differential.yml, differential.rs:18-29).
3. **Real-client compatibility proof.** redis-py, go-redis, and ioredis run in CI against both single-node and a 3-node Raft cluster, including RESP3, pub/sub, pipelining, CLUSTER SLOTS discovery, and MOVED routing, 54 PASS / 0 FAIL (.github/workflows/driver-matrix.yml, tests/drivers/DRIVER_MATRIX.md).
4. **Production-grade supply chain and release integrity.** Committed Cargo.lock, cargo-deny gate, actions pinned by commit SHA, cargo-auditable embedded SBOM plus CycloneDX export, minisign detached signature over SHA256SUMS, keyless Sigstore attestation, distroless nonroot image (release.yml:161-229, rust.yml:184, Dockerfile).
5. **A hardened, memory-safe protocol surface.** ironcache-protocol is #![forbid(unsafe_code)] with checked-arithmetic length parsing, pre-allocation caps, and iterative (not recursive) frame handling with 50k-frame flood regression tests (decode.rs:28-45, :161-181, :592-653); workspace-wide unsafe is confined to three crates with per-block SAFETY justifications.
6. **Timing-safe, revocation-correct auth.** SHA-256 at rest, black_box-hardened constant-time compare, single-source-of-truth pre-auth allow-list, and live deauth-plus-disconnect on ACL DELUSER (dispatch.rs:2403-2415, :500, :544-547).
7. **Crash-safe persistence.** Atomic tmp/fsync/rename with manifest-written-last commit protocol, per-file CRC, torn-file and truncated-tail rejection tests, and reshard-on-load across shard-count changes (format.rs:192-341, persist/lib.rs:539, coordinator.rs:1110-1144).
8. **A verified single-node upgrade command.** `ironcache upgrade` does sha256/minisign verify, write freeze, fsync'd SAVE-first, atomic swap, health gate with restart proof, and auto-rollback, with 19 seam-mocked tests and a recorded live-AWS validation (crates/ironcache/src/upgrade/mod.rs:1-33, README.md:157-173).
9. **Docs-as-infrastructure governance.** 31 ADRs with a CI-enforced index and weekly decision-issue reconciliation, 81 design specs, 98% rustdoc coverage on 1,815 pub items, only 6 TODO/FIXME markers across 185k lines (scripts/ci/check-adr-index.sh, adr-governance.yml).
10. **Operator-honest deployment docs.** DEPLOY.md documents real config keys, probe semantics, RPO, and explicitly separates "validated offline" from "needs a live cluster" (DEPLOY.md:388-425); `ironcache check` is a real nginx-t-style config preflight (main.rs:306-336).

---

## 4. Hardening Roadmap

**No P0 findings were confirmed.** Each item below was re-verified against the code + git history for this refresh. All 8 M1 P1s, the two P1s that carried the (previously n/s) scalability dimension, and the M2 latency-histogram P1 are now SHIPPED; the remaining OPEN items are P2/P3. Severity adjustments made by the original adversarial verifier are preserved (rolling-upgrade P1 to P2, config-rollback P1 to P3, god-files P1 to P3).

### M1 -- Trust blockers -- ALL SHIPPED

1. **[P1, Security] Add a total query-buffer cap. DONE (PR #542, closes #528/#535; boot-validation parity PR #596).** The serve loop now reads `ctx.runtime.query_buffer_limit()` and CLOSES the connection when the per-connection read buffer exceeds it, on both datapaths (serve.rs:2108-2114 tokio, :2530-2533 io_uring); a slow-dribble multibulk is cut off at the cap. A single bulk-string length header over `proto-max-bulk-len` (default 512 MB, Redis-compatible) is a hard `invalid bulk length` protocol error decided from the header alone, so the claimed payload is rejected before it is ever reserved (reject-before-alloc). #596 also made boot reject `proto-max-bulk-len 0` (a zero ceiling would reject every value), matching the runtime setter.
2. **[P1, Scalability] Enforce the output-buffer limit inside the pipelined batch. DONE (PR #542, closes #529).** The cap is now read once per batch and enforced INSIDE the decode loop after each command's reply is appended, plus the original post-batch check, on both datapaths (serve.rs:1714-1723, :2312), so a single pipelined batch of large-reply commands is cut off mid-batch before it can OOM the host.
3. **[P1, Upgrade] Make format-version-mismatched snapshots loud, not silently empty. DONE (PR #540, closes #530).** `load` now classifies a well-formed-but-unknown version as `SnapshotLoadError::UnknownVersion` (format.rs:49, :69, :233, :301) instead of returning None, so a downgrade fails loudly rather than booting an empty cache.
4. **[P1, Monitoring + Troubleshooting] Aggregate INFO across shards. DONE (PR #545, closes #531).** INFO now routes through `MetricsRegistry::aggregate()` so stats/clients/keyspace report node-wide totals that match /metrics and DBSIZE (dispatch.rs:383). Residual (out of #531's scope, documented at dispatch.rs:400): the per-command COMMANDSTATS table is not yet cross-shard-aggregated.
5. **[P1, Scalability] File-descriptor budgeting at boot. DONE (PR #539, closes #532).** Boot now reads RLIMIT_NOFILE and clamps effective maxclients to the fd limit with a loud log (Redis parity).
6. **[P1, Test] Timeout and diagnose the merge-gate deadlock. DONE (PR #537, closes #533).** The workspace test run now executes under cargo-nextest, which fixed the load-dependent deadlock; separately the DST sim crates are compiled opt-level 3 in dev/test so a seed sweep no longer looks like a hang (Cargo.toml `[profile.dev.package.*]`).
7. **[P1, Test + Security] Ship the documented parser fuzz gate. DONE (PR #541, closes #534).** A cargo-fuzz target over `ironcache_protocol::decode` now runs in CI, and the decode panic it found was fixed in the same PR.
8. **[P1, Maintainability] Rewrite CONTRIBUTING.md. DONE (PR #538, closes #536).** CONTRIBUTING.md now documents the implementation-phase build/test loop and the real merge gates in present tense.

### M2 -- Operator experience

9. **[P1, Monitoring] Command-latency histogram. DONE (PR #547, closes #546).** `ironcache_command_duration_seconds_{bucket,sum,count}` is now exported from /metrics, so p99 is graphable under load.
10. **[P2, Upgrade] Manual rolling-upgrade runbook, then the live driver. RUNBOOK DONE (PR #565, closes #562); LIVE DRIVER STILL OPEN.** The replica-first/FAILOVER/primary-last procedure is now written into docs/UPGRADE.md. The live cluster-upgrade wire I/O is NOT landed: cluster_upgrade.rs is still the PURE OBSERVERS only, its I/O half is an explicit "following slice" (cluster_upgrade.rs:1-16), there is no `ironcache cluster-upgrade` CLI verb, and #392 remains OPEN with no live end-to-end proof.
11. **[P2, Upgrade] Mixed-version compatibility CI. OPEN.** No CI job boots vN against vN-1; the additive-discriminant wire posture is still asserted only in comments (verified: no compat / 2-node old-vs-new job under .github/workflows/).
12. **[P2, Upgrade] Cut the first formal v0.x release. DONE (PR #566 + #567, closes #386).** The CHANGELOG Unreleased section is promoted under 0.1.0, a `v0.1.0` tag exists (git tag), and the production upgrade minisign key is pinned as PINNED_UPGRADE_PUBLIC_KEY (PR #567).
13. **[P2, Troubleshooting] Operator runbook. DONE (PR #552, closes #550).** docs/RUNBOOK.md now maps operator-visible errors and probe states to diagnostic sequences.
14. **[P2, Troubleshooting] Crash ergonomics. DONE (PR #553, closes #551).** The release profile now uses `strip = "debuginfo"` to keep the symbol table (Cargo.toml:194-217), a panic hook prints version + file:line + report URL (panic_hook.rs), and RUST_BACKTRACE is documented, so a forced panic yields a symbolized location.
15. **[P2, Monitoring] Replication metrics in /metrics plus honest INFO fields. DONE (PR #554, closes #549).** `instantaneous_ops_per_sec` is now a real node-wide recent rate (dispatch.rs:3786-3798), `rdb_last_bgsave_status` is emitted (dispatch.rs:3752), and repl lag/link gauges are exported. Residual: `total_net_input/output_bytes` are still not emitted (minor INFO parity gap).
16. **[P2, Monitoring] Ship dashboard, alert rules, and metric reference; default metrics on. DONE (PR #558, closes #555).** deploy/ now carries a Grafana dashboard + Prometheus alert rules, METRICS.md catalogs the series, and metrics default on (localhost) per the tunability principle.
17. **[P2, Security] Loud warning for unauthenticated cluster bus. DONE (PR #560, closes #557).** Booting a clustered mode without cluster_secret/cluster_tls now emits a prominent boot warning.
18. **[P2, Troubleshooting] Fail on unknown IRONCACHE_* env vars. DONE (PR #560, closes #557).** An unknown IRONCACHE_ key now WARNS at boot rather than silently keeping the default.
19. **[P2, Test] Pin the differential oracle and measure coverage. OPEN.** differential.yml still installs an UNPINNED `redis-server` from the Ubuntu archive (differential.yml:71-79) with no committed version table, and no coverage tooling (cargo-llvm-cov) exists in any workflow.
20. **[P2, Security] Fix TLS docs or ship the features. DONE (docs PR #564 closes #561; hot reload PR #584 closes #563).** The stale `docs/design/TLS.md` promised restart-free cert reload and client-port mTLS; the acceptor was built once at boot with `.with_no_client_auth()`. The docs are reconciled (a "Shipped status" section plus a new operator guide `docs/TLS.md`), AND the hot-reload follow-up has since SHIPPED: a SIGHUP handler over an ArcSwap `ReloadableAcceptor` re-reads the configured cert/key with no restart and no dropped connections (main.rs:335-337, serve.rs:206-217, :690). Client-port mTLS remains unavailable (out of scope).
21. **[P2, Upgrade] Document and log socket activation. DONE (PR #592 + #565, closes #389).** The LISTEN_FDS / LISTEN_FDNAMES conventions are parsed (listen_fds.rs), socket activation is documented in docs/UPGRADE.md, and boot logs state which path (adopted inherited fd vs self-bind fallback) was taken (listen_fds.rs:180-196).
22. **[P2, Troubleshooting] Coordinator observability. DONE (PR #559, closes #556).** hops-sent/served counters and inbox-depth gauges now appear in the ironcache_shard_* families, doubling as the #517 zero-hop measurement harness.

### M3 -- Polish (P2/P3)

23. **[P3, Upgrade] Config rollback escape hatch. OPEN.** No `ignore_unknown_config_keys` bootstrap option exists (verified: no such key in crates/), so deny_unknown_fields can still make the previous binary refuse to boot after a new config key is adopted, potentially crash-looping auto-rollback (narrow trigger window).
24. **[P3, Maintainability] Split the god files. OPEN.** The server crate's command-handler surface was extracted into per-type cmd_*.rs modules + route.rs, but the cited god files persist: the binary serve.rs is 9,575 lines (route_and_dispatch still at serve.rs:3499), dispatch.rs is 10,735 lines, and raft lib.rs is 10,124 lines.
25. **[P2, Test] Property tests. OPEN.** Still absent (verified: no proptest/quickcheck dependency in the workspace); promised in TESTING.md. Start with Value encode/decode round-trip and decode-never-panics on arbitrary bytes in ironcache-protocol.
26. **[P3, Security] overflow-checks = true in the release profile. OPEN.** `[profile.release]` does not set overflow-checks (Cargo.toml:194-217); the flag is only preserved in the dev/test profile. Cheap defense-in-depth for an I/O-bound cache.
27. **[P3, Monitoring] Real run_id / redis_mode. OPEN.** run_id is still the 40-zero placeholder (observe/lib.rs:2073-2075) and redis_mode is hardwired `standalone` (observe/lib.rs:1828). MONITOR is intentionally unimplemented (it returns "MONITOR is not supported"), which is honest: README.md:143 documents secret redaction for SLOWLOG, INFO, and logs (NOT MONITOR), so there is no doc mismatch to fix here.
28. **[P3, Upgrade] listener_for should honor LISTEN_FDNAMES and reject extra inherited fds. DONE (PR #592, part of #389).** listen_fds.rs now parses LISTEN_FDNAMES, maps names to fds, adopts only the fd named `resp` (listen_fds.rs:280), and returns an Err (do NOT adopt, self-bind) on a name/count mismatch rather than hanging (listen_fds.rs:43-129).
29. **[P3, Troubleshooting] Treat any shard-thread exit before shutdown as fatal. OPEN (partial).** A panicked shard thread is now surfaced as a non-zero exit at join (main.rs:362-364), but a shard thread that exits DURING serving still appears to leave /livez latched true; no evidence a mid-run liveness demotion was added.

### GENUINELY REMAINING (verified still open -- the real trust work)

None are P0 or P1. In rough priority order:

- **Live cluster rolling-upgrade driver + E2E (#392, #391).** The runbook shipped, but the cluster_upgrade.rs wire I/O half and the #391 single-node client-listener cutover are not landed and no live rolling upgrade has been proven end to end; hands-off cluster upgrades are still manual.
- **Mixed-version compatibility CI (item 11).** Nothing automatically boots vN against vN-1; the wire-compat guarantee is comment-asserted only, so a regression would land silently.
- **Pin the differential oracle + measure coverage (item 19).** The differential oracle is an unpinned Ubuntu redis-server (silent oracle drift) and there is no coverage tooling to find untested surface.
- **Config-rollback escape hatch (item 23).** deny_unknown_fields can crash-loop an auto-rollback onto the previous binary (narrow trigger, but the failure mode is a down node).
- **God-file splits (item 24).** dispatch.rs (10.7k), raft lib.rs (10.1k), and the binary serve.rs (9.6k) each remain a review-and-onboarding drag.
- **Property tests (item 25).** Promised in TESTING.md, absent; the decode-never-panics invariant is the highest-value first target.
- **overflow-checks in the release profile (item 26).** Cheap defense-in-depth, still off in release.
- **run_id / redis_mode honesty (item 27).** A placeholder run_id and hardwired `standalone` mode mislead exporters. MONITOR is intentionally unimplemented (returns "not supported") and README's redaction note covers SLOWLOG/INFO/logs, not MONITOR, so there is no doc mismatch there.
- **Shard-thread mid-run liveness (item 29).** A serving-time shard exit may leave /livez latched true, masking a limping node.
- **Minor INFO parity residuals.** total_net_input/output_bytes are not emitted and the per-command COMMANDSTATS table is not cross-shard-aggregated (documented follow-ups to items 15 and 4).

---

## 5. Notable Refuted Claims (excluded from the gap list)

- **"Tail-latency/determinism goal is 0% started" -- REFUTED.** The repo already contains the measurement infrastructure the epic's sub-task calls for (wrk2-style coordinated-omission-correct load generation in-tree); "0% started" is contradicted by in-tree evidence.
- **"Issue-tracker hygiene makes goal completion unmeasurable" -- REFUTED.** The quoted issues are open, but the thesis that no residual scope is stated did not survive verification.
- **Severity downgrades for the record:** rolling cluster upgrade P1 to P2 (correct sequence is encoded and testable, only the driver/runbook is missing), config rollback trap P1 to P3, god-files P1 to P3 (line counts inflated by end-of-file test modules).

---

## 6. Goal-Completion Verdict

**Standing goal:** implement all open GH issues, resolve all issues/PRs, and outperform Dragonfly or land within 3%, while dominating tail latency/determinism.

**Verdict: MET on GET throughput; the tail is competitive, not category-leading.** The issue-resolution half is largely done (the two "hygiene breaks the goal" claims were refuted), and the competitive question is now resolved by a corrected re-bench:

1. **The scoreboard is now corrected and IronCache LEADS GET.** The earlier apparent GET deficit (README's deep-pipeline 16-vCPU run bolding Dragonfly) was a benchmark CONFIG artifact -- IronCache's shards were oversubscribed relative to cores, so cross-shard hops depressed its GET. A corrected thread-per-core re-bench (c7g, 2026-07-10, proper shard-to-core placement) reverses it: IronCache leads GET by about 19% single-endpoint (2.43M vs Dragonfly 2.04M ops/sec) and by roughly 2x cluster-aware (4.32M vs 2.19M) once #517 zero-hop routing removes the cross-shard hop. #507 is CLOSED.
2. **The #517 hop-elimination chain is COMPLETE and paid off.** PR #525 merged (N-shard CLUSTER SLOTS projection + MOVED, eliminating the hop for cluster-aware clients) and PR #569 landed the driver-matrix cluster leg proving the hop is eliminated; issue #517 is CLOSED. The zero-hop routing is what delivers the roughly 2x cluster-aware GET lead above.
3. **Upgrade epic #385 is mostly landed but two phases lack live E2E:** #388/#389/#390 are CLOSED; #391 (streamed-handoff core landed via PR #594/#595, live client-listener cutover still open) and #392 (observers + pure driver landed, live cluster E2E still open) remain OPEN.

**Concrete finish line:**
1. DONE: the corrected thread-per-core re-bench (c7g, 2026-07-10) ran both legs -- single-endpoint redis-benchmark and cluster-aware against shard-owners -- and showed IronCache leading GET (+19% single-endpoint, ~2x cluster-aware via #517 zero-hop); #507 is CLOSED.
2. Keep the tail-latency story honest: the baseline p99.9 TIES Dragonfly (~15ms) and the durable-save tail (291ms, per the #588 per-slot Arc-COW) is competitive but not category-leading; do not oversell it. The remaining GET headroom (per-op allocation removal in the datapath) is tracked optimization, not a blocker.
3. Close #392 via one live end-to-end rolling upgrade on the existing 3-node harness (the runbook exists; the wire-I/O driver is the missing slice), and land or explicitly schedule the #391 live cutover on #385.

The competitive goal is met and provable on GET throughput, memory, and determinism; the only genuinely open trust work is the live cluster rolling-upgrade E2E (#392/#391), not the perf question.
