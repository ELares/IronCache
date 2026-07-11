// SPDX-License-Identifier: MIT OR Apache-2.0
//! The IronCache binary's library surface.
//!
//! The crate ships as one binary (`src/main.rs`), but the server wiring lives in this
//! library half so integration tests (`tests/`) can boot the REAL `run_server` (the
//! actual SO_REUSEPORT thread-per-core topology + the cross-shard coordinator), rather
//! than reimplementing a serve loop. `main.rs` consumes the same modules.
//!
//! Determinism / shared-nothing invariants (ADR-0002/0003) are unchanged: this is the
//! same code the binary runs, just reachable by name for tests.

#![forbid(unsafe_code)]

/// Persist-thread CPU pinning glue (#589): apply the `persist-cpu` knob to the current thread so a
/// save's off-core encode runs on a DEDICATED persist core instead of stealing a pinned datapath
/// serving core. The safe orchestration only (parse the knob + select cpus + call the runtime pin);
/// the `sched_setaffinity` `unsafe` lives in the `ironcache-runtime` seam, keeping this binary
/// `#![forbid(unsafe_code)]`. A graceful no-op when unset (default), on non-Linux, or on a bad core.
pub mod affinity;
/// BLOCKING-command parking (PROD-9): the per-shard FIFO WAITER REGISTRY a connection parks on
/// when a blocking list/zset pop (BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP) finds every
/// key empty. A push to a waited key WAKES the longest-waiting waiter (Redis fairness); the woken
/// connection re-attempts the pop and either succeeds or re-parks. The parse + the non-blocking
/// ATTEMPT live in `ironcache-server::cmd_block` (runtime-agnostic); the parking + the timer arm +
/// the registry (which need tokio's `Notify` + the runtime timer seam) live here.
pub mod blocking;
pub mod coordinator;
/// The out-of-band operations HTTP endpoint (OBSERVABILITY.md, #152): a bounded, hand-rolled
/// tokio HTTP/1.1 responder on `--metrics-addr` serving Prometheus `/metrics`, `/livez`, and
/// `/readyz`. Enabled by DEFAULT on a localhost bind (#555, `127.0.0.1:9091`); disable it with
/// `--metrics-addr off`.
pub mod metrics_http;
pub mod multikey;
/// Boot-time PANIC HOOK for crash ergonomics (#551): a process-wide hook installed once at boot that
/// logs (via `tracing`) the panic message + location + build version + a "report at <issues URL>"
/// line before the `panic = "abort"` release build aborts, so even a crashing process leaves
/// actionable last words. Boot/panic-path, outside the ADR-0003 determinism boundary.
pub mod panic_hook;
/// Durable on-disk SNAPSHOT persistence serve wiring (#58): the cross-shard SAVE/BGSAVE fan-out,
/// the manifest commit, LASTSAVE, load-on-boot, and the periodic save policy. Default-off (only
/// engaged when a `data_dir` is configured); the engine half lives in `ironcache-persist`.
pub mod persist;
// The upgrade-handoff snapshot staging target + its RAM-headroom guard (#390): stage the handoff
// snapshot on tmpfs (/dev/shm) to remove the disk I/O, but ONLY when it fits in available RAM with
// headroom (tmpfs is RAM -- a too-big snapshot would OOM), else the durable data_dir. Pure decision +
// a Linux MemAvailable read; the OOM-prevention guard is the correctness core.
pub mod handoff;
// The live rolling-upgrade OBSERVERS (#392 Phase 3): translate a cluster snapshot (ClusterView) into
// the ironcache-repl upgrade_step inputs + pick the promotion candidate. The pure observe/decision
// layer of the live UpgradeActions impl; the wire I/O (fetch /topology + INFO, CLUSTER FAILOVER, the
// per-node upgrade) is a following slice.
pub mod cluster_upgrade;
// The LIVE clustered rolling-upgrade DRIVER (#392 Phase 3): the `impl UpgradeActions` (LiveCluster)
// that assembles a per-tick ClusterView from the authenticated RESP surface (INFO / CLUSTER SHARDS /
// CLUSTER INFO), drives the three act methods over the wire, and implements the failover-freeze fence
// (CLIENT PAUSE WRITE on the old primary -> drain the candidate to lag 0 -> CLUSTER FAILOVER, fail
// closed on a drain timeout) as the load-bearing RPO=0 mechanism. The per-node binary swap is behind
// the NodeUpgrader trait (prod: SSH-invoke the single-node `ironcache upgrade`); the CLI wiring + the
// live 3-node acceptance test are following slices.
pub mod cluster_upgrade_driver;
pub mod pubsub;
pub mod raft_boot;
/// HA-7d LIVE per-shard replica attach. Reached ONLY in raft-mode once an `AssignReplica`
/// naming this node is committed; the default static path never touches it. `pub(crate)`:
/// internal serve wiring, not a client surface.
pub(crate) mod replica_attach;
pub mod serve;
pub mod spanning_combine;
/// Home-core ATOMIC apply for the SHARD-SPANNING src->dst move commands (SMOVE/LMOVE/
/// RPOPLPUSH) + the spanning all-or-nothing MSETNX (COORDINATOR.md #107, the PROD-9
/// cross-shard atomicity slice). Ends the prior SILENT home-subset partial-apply for these
/// commands; spanning RENAME/RENAMENX/COPY/LMPOP/ZMPOP/SORT-STORE are FAIL-LOUD instead.
pub mod spanning_move;
pub mod test_support;
/// The structured topology read endpoint (#365): the versioned JSON `/topology` document the
/// console reads membership/slots/epoch/raft state from, coherent in standalone mode.
pub mod topology;
/// TURNKEY cluster formation (PROD-turnkey): on a FRESH raft cluster the elected leader auto-applies
/// the shipped static `cluster_topology`'s node table + slot ownership through the Raft log, so a
/// deploy reaches `cluster_state:ok` with all slots assigned WITHOUT a manual `CLUSTER MEET` /
/// `ADDSLOTS`. Idempotent + fresh-only (never re-bootstraps / clobbers a committed config).
pub mod turnkey_bootstrap;
/// `ironcache upgrade`: the operator-run, verified (sha256), data-safe (SAVE-first), health-gated,
/// auto-rolling-back binary self-updater (#387 mechanism). The signature anchor (#386), HTTPS
/// auto-fetch, and the lossless write-freeze (#388) are explicit follow-ups with seams left here
/// (the `Verifier`/`BinarySource` traits). Operator-run + privileged; NEVER a RESP surface.
pub mod upgrade;
pub mod whole_keyspace;
