// SPDX-License-Identifier: MIT OR Apache-2.0
//! The IronCache server layer: per-connection state and Tier-0 command dispatch
//! (PROTOCOL.md, COMMANDS.md, CONNECTION_LIFECYCLE.md).
//!
//! This crate is runtime-agnostic and time-agnostic: it takes a [`ironcache_env`]
//! clock and never touches the OS clock, sockets, or RNG directly (ADR-0003). The
//! binary wires it to the concrete runtime and Env.
//!
//! Entry points:
//! - [`ConnState`] - per-connection state (proto, db, name, auth, id).
//! - [`ServerContext`] - immutable server-wide facts (password, databases, info).
//! - [`dispatch`] - map a parsed request to a reply, mutating state as needed.

#![forbid(unsafe_code)]

pub mod acl;
pub mod admission;
pub mod cmd_acl;
pub mod cmd_bitmap;
pub mod cmd_block;
pub mod cmd_cluster;
pub mod cmd_config;
pub mod cmd_expire;
pub mod cmd_hash;
pub mod cmd_hll;
pub mod cmd_introspect;
pub mod cmd_keyspace;
pub mod cmd_list;
pub mod cmd_set;
pub mod cmd_sort;
pub mod cmd_string;
pub mod cmd_txn;
pub mod cmd_util;
pub mod cmd_zset;
pub mod command_spec;
pub mod conn;
pub mod dispatch;
pub mod glob;
pub mod notify;
pub mod route;

// The ACL engine (#106): the runtime-mutable user registry + the user model + the rule
// parser + the per-command/key/channel category map. `AclState` is carried by
// `ServerContext`; the serve layer reads it for AUTH + per-command enforcement.
pub use acl::{AclParseError, AclResolution, AclState, Category, DEFAULT_USER, User};
// The `ACL` command family (#106): the serve layer calls `dispatch_acl` to run the ACL
// admin verbs (WHOAMI/LIST/USERS/GETUSER/SETUSER/DELUSER/CAT/GENPASS/SAVE/LOAD) and acts on
// the returned `AclSideEffect` (the aclfile SAVE/LOAD I/O it owns).
pub use cmd_acl::{AclSideEffect, dispatch_acl};

// PROD-9 leader-hint: the serve-layer raft-mode CLUSTER mutator resolves which node is the raft
// LEADER (and its advertised CLIENT endpoint) for a NOTLEADER redirect, so an operator who hit a
// FOLLOWER knows where to reissue. `resolve_leader_hint` reads the leader the `RaftHandle`
// recognizes and maps it to the leader's client `host:port` via the committed slot map.
pub use cmd_cluster::{LeaderHint, SlotScan, parse_slot_scan, resolve_leader_hint};

pub use admission::is_denyoom;
// The BLOCKING command family (PROD-9): the serve layer parses a blocking command into a
// [`cmd_block::BlockSpec`] (timeout + keys + op), ATTEMPTS the non-blocking op via
// [`cmd_block::try_block_op`], and PARKS the connection on a per-shard FIFO waiter registry
// when every key is empty. `is_blocking_command` is the router's interception gate;
// `wake_keys_for_write` tells the serve layer which destination key(s) a write may have made
// ready (so it wakes a parked waiter); `block_timeout_reply` is the nil-array timeout reply.
pub use cmd_block::{
    BlockOp, BlockSpec, BlockTimeoutMs, block_timeout_reply, cmd_block_nonblocking,
    is_blocking_command, parse_block, try_block_op, wake_keys_for_write,
};
// The PURE set-algebra combiner + its op enum (the single source of truth shared by the
// single-shard set handlers and the cross-shard coordinator's gather-combine, COORDINATOR.md
// #107, Stage 2b), and the INTERNAL `__ICSTORESET` dest-write verb token (client-unreachable;
// only the coordinator issues it to write a spanning *STORE result to the dest owner).
pub use cmd_set::{ICSTORESET, SetOp, set_combine};
// The PURE zset-algebra combiner + its op / aggregate enums (the single source of truth
// shared by the single-shard zset handlers and the cross-shard coordinator's gather-combine,
// COORDINATOR.md #107, Stage 2b-2), and the INTERNAL `__ICSTOREZSET` dest-write verb token
// (client-unreachable; only the coordinator issues it to write a spanning zset *STORE /
// ZRANGESTORE result to the dest owner).
pub use cmd_zset::{AggOp, Aggregate, ICSTOREZSET, ScoredMember, WeightedSource, zset_combine};
// The PURE BITOP combiner (the single source of truth shared by the single-shard BITOP
// handler and the cross-shard coordinator's gather-combine, COORDINATOR.md #107, Stage
// 2b-3). The cross-shard BITOP reuses a plain routed `SET dest <bytes>` for its dest write
// (SET clears the dest TTL by default, matching BITOP's blind-overwrite-clear-TTL), so
// there is NO internal BITOP write verb.
pub use cmd_bitmap::{bitop_compute, bitop_validate_op};
// The PURE HyperLogLog union + estimator primitives (the single source of truth shared by
// the single-shard PFCOUNT/PFMERGE handlers and the cross-shard coordinator's
// gather-union-estimate, COORDINATOR.md #107, Stage 2b-3), plus the INTERNAL `__ICSTOREHLL`
// dest-write verb token (client-unreachable; only the coordinator issues it to write a
// merged HLL to the PFMERGE dest owner with the dest TTL PRESERVED).
pub use cmd_hll::{
    HLL_REGISTERS, ICSTOREHLL, dense_from_regs, estimate_reply, is_valid_dense, merge_into,
    regs_reghisto,
};
// The #89 single-source-of-truth command registry. `CommandClass` is also re-exported via
// `route` (its legacy path) below; `Arity` is re-exported via `cmd_txn` (its legacy path).
pub use command_spec::{
    Arity, CommandSpec, ICCOUNTKEYSINSLOT, ICEXISTS, ICGETKEYSINSLOT, ICPUBLISH, ICPUBSUB,
    ICSPUBLISH, KeySpecKind, is_write, request_is_write_for_pause, spec_of,
};
pub use conn::ConnState;
pub use dispatch::{
    CmdStatsFn, EXPIRE_CYCLE_INTERVAL, MAX_RECLAIM_PER_CALL, MAX_RECLAIM_PER_CYCLE, RollupFn,
    ServerContext, ShutdownMode, acl_enforce, acl_resolve_if_stale, command_allowed_pre_auth,
    dispatch, dispatch_remote_keyed, dispatch_remote_whole_keyspace, dispatch_with_cmd,
    drain_due_keys, in_sync_replica_count, parse_shutdown,
};
pub use route::{
    CommandClass, KeySpec, classify, command_keys, owner_shard, owner_shard_set, single_key,
};

// Re-export the Raft control-plane handle (HA-4c) carried by `ServerContext::raft`, so the
// serve layer can name it via `ironcache_server::RaftHandle` without taking its own raft-net
// dependency edge. `ProposeOutcome` is the typed result a raft-mode CLUSTER mutator maps to
// `+OK` / `-CLUSTERDOWN`. `MembershipOutcome` + `ClusterConfig` carry the operator-driven dynamic
// Raft membership path (HA-prod-membership): MEET -> AddLearner, auto-promote, FORGET -> RemoveVoter.
pub use ironcache_raft_net::{
    ClusterConfig, MembershipOutcome, ProposeOutcome, RaftHandle, Status,
};

// Re-export the replication STATUS types (HA-7e) carried by `ServerContext::repl_status`, so the
// serve layer (and the binary's repl tasks that publish to it) can name them via
// `ironcache_server::{ReplNodeStatus, ...}` without each taking its own repl dependency edge. The
// INFO `# Replication` section + CLUSTER SHARDS render from a `ReplStatusSnapshot`; HA-8 consumes
// `replica_is_in_sync` / `ReplNodeStatus::is_in_sync` as the promotion gate.
pub use ironcache_repl::{
    InSyncReplicas, LinkStatus, ReplId, ReplNodeStatus, ReplRole, ReplStatusSnapshot, ReplicaLag,
    replica_is_in_sync,
};

// Re-export the observe types the binary supplies to dispatch (the INFO memory
// snapshot it reads once at the binary edge, and the per-command counter deltas the
// serve loop folds into the shard counters after dispatch returns).
pub use ironcache_observe::{CounterDeltas, MemoryInfo};

// Re-export the per-shard timing wheel the binary owns (Rc<RefCell<>>) and passes to
// dispatch for the active TTL drain + deadline registration (#51).
pub use ironcache_expiry::TimingWheel;

// Re-export the protocol types callers need so the binary depends on one crate
// for the server surface.
pub use ironcache_protocol::{
    DecodeOutcome, ErrorReply, Limits, ProtoVersion, Request, Value, decode, encode,
};

// Re-export the storage WAIST types the binary needs to construct/pass a store and
// the `now` basis. The binary depends on ironcache-storage transitively through
// here for the trait, and on ironcache-store directly for the concrete impl. `Admit`
// is the PR-3a admission surface dispatch bounds on (evict-to-fit + policy queries);
// `ActiveExpiry` is the PR-3b active-drain surface (reap_if_expired). `Watch` is the
// PR-10b WATCH optimistic-lock surface (watch_snapshot/watch_is_dirty/unwatch) dispatch
// bounds on; `WatchEntry` is the per-key snapshot the connection holds.
pub use ironcache_storage::{
    ActiveExpiry, Admit, Keyspace, ScanCursor, Store, UnixMillis, Watch, WatchEntry,
};
