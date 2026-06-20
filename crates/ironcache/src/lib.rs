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
/// `/readyz`. Spawned ONLY when `--metrics-addr` is set; default-off boot is byte-identical.
pub mod metrics_http;
pub mod multikey;
/// Durable on-disk SNAPSHOT persistence serve wiring (#58): the cross-shard SAVE/BGSAVE fan-out,
/// the manifest commit, LASTSAVE, load-on-boot, and the periodic save policy. Default-off (only
/// engaged when a `data_dir` is configured); the engine half lives in `ironcache-persist`.
pub mod persist;
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
/// TURNKEY cluster formation (PROD-turnkey): on a FRESH raft cluster the elected leader auto-applies
/// the shipped static `cluster_topology`'s node table + slot ownership through the Raft log, so a
/// deploy reaches `cluster_state:ok` with all slots assigned WITHOUT a manual `CLUSTER MEET` /
/// `ADDSLOTS`. Idempotent + fresh-only (never re-bootstraps / clobbers a committed config).
pub mod turnkey_bootstrap;
pub mod whole_keyspace;
