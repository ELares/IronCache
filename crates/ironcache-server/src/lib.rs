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

pub mod admission;
pub mod cmd_bitmap;
pub mod cmd_config;
pub mod cmd_expire;
pub mod cmd_hash;
pub mod cmd_hll;
pub mod cmd_introspect;
pub mod cmd_keyspace;
pub mod cmd_list;
pub mod cmd_set;
pub mod cmd_string;
pub mod cmd_txn;
pub mod cmd_util;
pub mod cmd_zset;
pub mod conn;
pub mod dispatch;
pub mod glob;
pub mod route;

pub use admission::is_denyoom;
pub use conn::ConnState;
pub use dispatch::{
    EXPIRE_CYCLE_INTERVAL, MAX_RECLAIM_PER_CALL, MAX_RECLAIM_PER_CYCLE, RollupFn, ServerContext,
    dispatch, dispatch_remote_keyed, dispatch_remote_whole_keyspace, dispatch_with_cmd,
    drain_due_keys,
};
pub use route::{
    CommandClass, KeySpec, classify, command_keys, owner_shard, owner_shard_set, single_key,
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
pub use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, Request, Value, decode, encode};

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
