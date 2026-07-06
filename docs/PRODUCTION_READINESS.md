# IronCache Production Readiness Report

Synthesis of 8 dimension audits plus adversarial verification of every P0/P1 finding. All claims below are evidence-cited (file:line or issue/PR number); refuted findings are excluded from the gap list and recorded in section 5.

---

## 1. Executive Verdict: CONDITIONALLY READY

IronCache is conditionally ready: a single-node, authenticated deployment on a trusted network, supervised by systemd or Kubernetes with the shipped manifests, is defensible today, backed by a genuinely strong correctness suite (2,239 tests, deterministic-simulation Raft verification, a merge-gating differential test against live redis-server, a 3-client driver matrix) and production-grade supply-chain discipline (committed Cargo.lock, cargo-deny, SHA-pinned actions, SBOM, minisign plus Sigstore). It is not ready for internet-exposed listeners, monitoring-dependent fleets, or hands-off cluster operations: adversarial verification confirmed ten P1 gaps, headlined by two pre-auth/pre-cap memory-exhaustion vectors (unbounded query buffer at serve.rs:1940, unbounded intra-batch reply accumulation at serve.rs:1598-1828), a snapshot format mismatch that silently boots an empty cache (format.rs:207, coordinator.rs:1164), and an INFO implementation that reports roughly 1/N-of-node statistics to every Redis monitoring tool (serve.rs:6064). No P0 was confirmed, every P1 is a bounded, well-located fix, and the honest documentation culture (THREAT_MODEL.md accepted-risk rows, DEPLOY.md "validated vs unvalidated" section) means the remaining work is closing real gaps, not discovering hidden ones.

---

## 2. Scorecard

| Dimension | Score /10 | Status |
|---|---|---|
| Test coverage & correctness assurance | 6.5 | Exceptional breadth (DST, differential gate, driver matrix) but misses its own declared bar: no fuzzing, no coverage measurement, CI gate has a known undiagnosed deadlock with no timeout |
| Security & public trust | 7.5 | Disciplined unsafe, timing-safe auth, hardened decoder, exemplary supply chain; one real pre-auth memory-exhaustion vector and doc-vs-reality gaps (fuzzing, TLS reload) |
| Monitoring & observability | 6.5 | Broad native Prometheus surface with real /readyz semantics, but INFO is shard-sliced (breaks redis_exporter/Grafana and its own console) and no latency histogram exists anywhere |
| Troubleshooting & operability | 6.5 | Rich diagnostic commands and excellent config-error UX; shard-sliced INFO, unsymbolizable crashes, and zero runbook keep it short of production grade |
| Upgrade & lifecycle safety | 6.0 | Single-node upgrade orchestrator is genuinely complete; beyond one node the story degrades (rolling upgrade not operator-executable, silent-empty dump load, no formal release ever cut) |
| Codebase maintainability & contributor scalability | 7.5 | 31 machine-checked ADRs, 98% doc coverage, clean crate graph; three god files and an actively false CONTRIBUTING.md are the drag |
| Service scalability & resource safety | n/s* | Two confirmed P1s: no RLIMIT_NOFILE budgeting at boot, and the output-buffer cap fires only after a whole pipelined batch (host-OOM window) |
| Goal completion audit | n/s* | Issue-resolution goal largely met; competitive goal unmet as last measured (24%/33% behind Dragonfly) and the current gap is unknown, pending re-bench |

*Numeric scores for these two dimensions were not transmitted in the synthesis inputs; their confirmed findings were, and are fully incorporated below.

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

**No P0 findings were confirmed.** All items below are verifier-confirmed P1s, then notable P2s, then P3 polish, grouped into three milestones. Severity adjustments made by the adversarial verifier are applied (rolling-upgrade P1 to P2, config-rollback P1 to P3, god-files P1 to P3).

### M1 -- Trust blockers (confirmed P1s: DoS, data safety, silently wrong numbers, public honesty)

1. **[P1, Security] Add a total query-buffer cap.** read_buf grows unbounded pre-auth (serve.rs:1940, 1953-1956); a `*1048576\r\n` header plus dribbled elements forces multi-GB allocation before any frame completes, and HARDENING.md:50-57 falsely claims an "accumulated-frame bound" exists. Fix: runtime-settable client-query-buffer-limit (default 1GB) checked in the serve loop, closing the connection on breach. Accept when: a slow-dribble multibulk test shows the connection closed at the cap with flat process RSS, and HARDENING.md matches the implemented bound.
2. **[P1, Scalability] Enforce the output-buffer limit inside the pipelined batch.** The cap fires only post-batch/pre-flush (serve.rs:1860-1863 tokio, :2284-2287 io_uring); intra-batch reply accumulation is unbounded and can OOM the host. Fix: compare out.len() against output_buffer_limit after each command's reply is appended; optionally flush past a soft high-water mark. Accept when: a single pipelined batch of large-reply commands is cut off mid-batch at the cap under a test, on both datapaths.
3. **[P1, Upgrade] Make format-version-mismatched snapshots loud, not silently empty.** format.rs:207/:267 return None on any version mismatch, persist/lib.rs:222-223 defines None as "start empty, never an error", and coordinator.rs:1164-1172 logs only when loaded > 0, so a downgrade silently discards all data. Fix: distinguish UnknownVersion from missing/torn, tracing::error at boot, optional refuse-to-start flag. Accept when: a FORMAT_VERSION+1 manifest/shard-file unit test passes, the error log fires, and a one-paragraph compat policy lands in DEPLOY.md.
4. **[P1, Monitoring + Troubleshooting] Aggregate INFO across shards.** serve.rs:6064 wires the serving shard's counters as INFO's rollup, so stats/clients/keyspace report ~1/N of the node, vary per connection, contradict DBSIZE (whole_keyspace.rs:5-13) and /metrics, and poison redis_exporter, Grafana, and IronCache's own console (console/info.rs:68-70). Fix: always build MetricsRegistry, route INFO through the existing MetricsRegistry::aggregate() (observe/lib.rs:384-389), fan # Keyspace through the whole-keyspace scatter-gather. Accept when: INFO totals equal /metrics totals and DBSIZE on a multi-shard node under test, and the stale comment at dispatch.rs:3704-3706 is corrected.
5. **[P1, Scalability] File-descriptor budgeting at boot.** Zero RLIMIT_NOFILE handling anywhere (exhaustive multi-convention search), and ironcache.service blocks @resources syscalls with no LimitNOFILE. Fix: getrlimit at boot, raise soft to hard or clamp effective maxclients with a loud log (Redis parity); LimitNOFILE=65535 in the unit. Accept when: booting with a low ulimit produces the clamp log and accepting maxclients connections never hits EMFILE mid-traffic.
6. **[P1, Test] Timeout and diagnose the merge-gate deadlock.** cargo test --workspace intermittently hangs ~50 min under load (maintainer-documented; corroborated by rust.yml:82-86 and commit 2b8a668), rust.yml has zero timeout-minutes, and a genuine server lock-ordering bug is not yet ruled out. Fix: timeout-minutes: 30 now; dedicated reproduction with thread backtraces to classify test-infra vs server bug; adopt cargo-nextest. Accept when: a hang can never burn a 6-hour runner, and the root cause is classified in writing (with a server fix if it is a real deadlock).
7. **[P1, Test + Security] Ship the documented parser fuzz gate.** TESTING.md:69-76 and HARDENING.md:49-50 both declare a merge-gating cargo-fuzz parser gate; none exists (verified: no fuzz/ dir, no fuzzing dep, no workflow), and release panic=abort (Cargo.toml:176) makes any missed panic a whole-process crash. Fix: cargo-fuzz target over ironcache_protocol::decode seeded from the differential corpus, bounded CI job. Accept when: the fuzz job runs in CI and TESTING.md/HARDENING.md describe only what actually runs.
8. **[P1, Maintainability] Rewrite CONTRIBUTING.md.** It states verbatim "there is no implementation code yet, by design" (CONTRIBUTING.md:3-5) against 185k lines of Rust, describing the CI gates in future tense. Fix: implementation-phase build/test instructions, actual merge gates, a first-command walkthrough, a documented Linux dev loop for io_uring work. Accept when: a new contributor can go from clone to green cargo test using only that file. (Half a day; highest onboarding ROI in the repo.)

### M2 -- Operator experience (remaining P1 plus notable P2s)

9. **[P1, Monitoring] Command-latency histogram.** No histogram/percentile exists anywhere in the server (only bench-crate code), which is disqualifying for a project whose stated strategy is tail-latency dominance. Fix: fixed log-spaced per-shard buckets of relaxed atomics exported as ironcache_command_duration_seconds_{bucket,sum,count}, reusing the elapsed-micros measurement SLOWLOG already takes. Accept when: p99 is graphable from /metrics under load.
10. **[P2, Upgrade] Manual rolling-upgrade runbook, then the live driver.** The cluster upgrade's wire I/O never landed (cluster_upgrade.rs:8-10); run_rolling_upgrade has only mock tests and no CLI verb. Fix: write the replica-first/FAILOVER/primary-last procedure (already encoded in upgrade_plan.rs:226) into DEPLOY.md now, land the `ironcache cluster-upgrade` I/O slice after. Accept when: one live rolling upgrade succeeds on the existing 3-node harness following the doc.
11. **[P2, Upgrade] Mixed-version compatibility CI.** The additive-discriminant wire posture (codec.rs:53-58) is asserted only in comments; nothing ever boots vN against vN-1. Fix: CI job pulling the previous rolling tarball for a 2-node compat smoke (old replica vs new primary and vice versa, dump written-by-new loads on old). Accept when: the job gates merges.
12. **[P2, Upgrade] Cut the first formal v0.x release.** 208 calendar tags, zero v* tags, a permanently-[Unreleased] CHANGELOG, unset upgrade public key; `ironcache upgrade --version latest` installs the newest unsoaked commit. Accept when: a v0.x tag exists, release.yml's minisign/SBOM path has run once, PINNED_UPGRADE_PUBLIC_KEY is set, and RELEASING.md states the interim compat policy.
13. **[P2, Troubleshooting] Operator runbook.** docs/ is explicitly a design record; no symptom-to-action guide exists for stuck /readyz, "ERR cross-shard target unavailable" (coordinator.rs:1705, mentioned in no doc), lost quorum, or full-disk save failures. Accept when: docs/RUNBOOK.md indexes every operator-visible error string and probe state with a diagnostic sequence.
14. **[P2, Troubleshooting] Crash ergonomics.** panic=abort plus strip="symbols" with no panic hook and zero RUST_BACKTRACE documentation leaves 3am aborts unsymbolizable. Fix: debug=1 with strip="debuginfo" (or published split debuginfo), a panic hook printing version plus report URL, DEPLOY.md crash section. Accept when: a forced panic in a release build yields a symbolized location.
15. **[P2, Monitoring] Replication metrics in /metrics plus honest INFO fields.** No repl series exists in the Prometheus path (observe/lib.rs:551-572); instantaneous_ops_per_sec is hardwired 0 (dispatch.rs:3719-3726); no rdb_last_bgsave_status, so the canonical "last save failed" alert is impossible. Accept when: repl lag/link gauges and a save-failures counter are exported and ops/sec is real.
16. **[P2, Monitoring] Ship dashboard, alert rules, and metric reference; default metrics on.** No Grafana JSON or Prometheus rules anywhere; the endpoint exists only with --metrics-addr. Accept when: deploy/ contains a starter dashboard plus rules file, a METRICS.md catalogs every series, and metrics default to localhost per the tunability principle.
17. **[P2, Security] Loud warning for unauthenticated cluster bus.** Default cluster modes run a plaintext, unauthenticated bus/replication stream (config/lib.rs:1141-1146, clusterbus/security.rs:9) with no boot warning; port+10000/+20000 exposure means consensus join or keyspace siphon. Accept when: boot without cluster_secret/cluster_tls in a clustered mode emits a prominent warn (or is rejected).
18. **[P2, Troubleshooting] Fail on unknown IRONCACHE_* env vars.** from_env reads a fixed list (config/lib.rs:1528-1560); a typo'd var silently keeps the default, the opposite of the strict TOML posture DEPLOY.md steers orchestrator users away from. Accept when: an unknown IRONCACHE_ key fails boot (or warns) naming the nearest valid name.
19. **[P2, Test] Pin the differential oracle and measure coverage.** The oracle is whatever redis-server Ubuntu ships (design doc requires pinned Valkey with a committed version table); no coverage tooling exists anywhere. Accept when: the oracle version is committed and bumped by PR, and an informational cargo-llvm-cov job publishes per-PR reports.
20. **[P2, Security] Fix TLS docs or ship the features.** TLS.md promises restart-free cert reload and mTLS; the acceptor is built once at boot (serve.rs:648) with .with_no_client_auth() (tls.rs:115). Accept when: either the acceptor rebuilds on reload signal, or TLS.md plainly states rotation needs a restart and mTLS is unavailable.
21. **[P2, Upgrade] Document and log socket activation.** DEPLOY.md never mentions LISTEN_FDS; the runtime never logs adopted-fd vs silent self-bind fallback (tokio_rt.rs:183), so the operator cannot verify the listen queue survived the exact restart the feature exists for. Accept when: DEPLOY.md has the section and boot logs state which path was taken.
22. **[P2, Troubleshooting] Coordinator observability.** The 1024-bounded cross-shard inbox backpressures invisibly (coordinator.rs:47-51); zero hop counters or depth gauges exist, despite hop elimination being the project's own strategic lever (#517). Accept when: hops-sent/served and inbox-depth appear in the ironcache_shard_* families. (This doubles as the #517 measurement harness.)

### M3 -- Polish (P2/P3)

23. **[P3, Upgrade] Config rollback escape hatch.** deny_unknown_fields makes the previous binary refuse to boot after a new config key is adopted, potentially crash-looping auto-rollback; verifier downgraded to P3 (narrow trigger window). Fix: ignore_unknown_config_keys bootstrap option plus a deprecated-key policy.
24. **[P3, Maintainability] Split the god files.** serve.rs (8.9k), dispatch.rs, raft lib.rs (10.6k), and the 917-line 17-parameter route_and_dispatch (serve.rs:3257-4173); mechanical module extraction along existing seams, one file per PR. Verifier downgraded from P1 (large fractions are test modules).
25. **[P2, Test] Property tests.** Promised in TESTING.md:80-82, absent. Start with Value encode/decode round-trip and decode-never-panics on arbitrary bytes in ironcache-protocol.
26. **[P3, Security] overflow-checks = true in the release profile.** Cheap defense-in-depth; negligible cost for an I/O-bound cache.
27. **[P3, Monitoring] Real run_id (currently 40 zeros, observe/lib.rs:1459-1461), redis_mode:cluster when clustered, MONITOR either implemented or documented absent (README.md:141 currently claims MONITOR redaction for a command that does not exist).**
28. **[P3, Upgrade] listener_for should honor LISTEN_FDNAMES and reject/close extra inherited fds (tokio_rt.rs:182-183); clients of unadopted sockets currently hang forever.**
29. **[P3, Troubleshooting] Treat any shard-thread exit before shutdown as fatal (bootstrap.rs:288-292 currently leaves a limping node with /livez latched true).**

---

## 5. Notable Refuted Claims (excluded from the gap list)

- **"Tail-latency/determinism goal is 0% started" -- REFUTED.** The repo already contains the measurement infrastructure the epic's sub-task calls for (wrk2-style coordinated-omission-correct load generation in-tree); "0% started" is contradicted by in-tree evidence.
- **"Issue-tracker hygiene makes goal completion unmeasurable" -- REFUTED.** The quoted issues are open, but the thesis that no residual scope is stated did not survive verification.
- **Severity downgrades for the record:** rolling cluster upgrade P1 to P2 (correct sequence is encoded and testable, only the driver/runbook is missing), config rollback trap P1 to P3, god-files P1 to P3 (line counts inflated by end-of-file test modules).

---

## 6. Goal-Completion Verdict

**Standing goal:** implement all open GH issues, resolve all issues/PRs, and outperform Dragonfly or land within 3%, while dominating tail latency/determinism.

**Verdict: partially met, current competitive position unknown.** The issue-resolution half is largely done (the two "hygiene breaks the goal" claims were refuted), but three confirmed P2 gaps remain:

1. **The scoreboard is stale.** The only 16-vCPU head-to-head has IronCache 24% behind on GET and 33% on SET (README.md:315-316), against an unpinned Dragonfly build, and no re-bench has run since the July-4 perf wave, so five merged levers are unquantified.
2. **The #517 hop-elimination chain is incomplete** (PR #525 open, PR5 unlanded), and epic #507's acceptance is internally inconsistent: the primary lever gives zero benefit under the single-endpoint methodology as written, making "within 3%" currently unfalsifiable.
3. **Upgrade epic #385 is half-landed:** #391 not started, #389/#390/#392 have merged cores but no closure criteria or live E2E proof.

**Concrete finish line:**
1. Merge #525, land PR5 (driver-matrix cluster leg plus zero-hop metric assertion).
2. Run one c7g.4xlarge re-bench of current main against Dragonfly pinned to v1.39.0 (docs/bench/COMPETITORS.md:22), executing BOTH legs: single-endpoint redis-benchmark (the #507 acceptance as written) and memtier --cluster-mode against shard-owners (the #517 acceptance), under the existing AWS teardown rules. One afternoon of instance time.
3. Amend #507 to name which leg defines "within 3%", so done can be declared or the remaining constant-factor work scoped.
4. Close #392 via one live end-to-end rolling upgrade on the existing 3-node harness; state residual scope on #389/#390 in one comment each and close if none; explicitly schedule or demote #391 on #385.

When steps 1-4 are done, the goal is either met and provable, or the remaining gap is a measured number with named levers, not an unknown.
