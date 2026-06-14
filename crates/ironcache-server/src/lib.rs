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

pub mod cmd_keyspace;
pub mod cmd_string;
pub mod cmd_util;
pub mod conn;
pub mod dispatch;

pub use conn::ConnState;
pub use dispatch::{RollupFn, ServerContext, dispatch};

// Re-export the observe types the binary supplies to dispatch (the INFO memory
// snapshot it reads once at the binary edge).
pub use ironcache_observe::MemoryInfo;

// Re-export the protocol types callers need so the binary depends on one crate
// for the server surface.
pub use ironcache_protocol::{DecodeOutcome, Limits, ProtoVersion, Request, Value, decode, encode};

// Re-export the storage WAIST types the binary needs to construct/pass a store and
// the `now` basis. The binary depends on ironcache-storage transitively through
// here for the trait, and on ironcache-store directly for the concrete impl.
pub use ironcache_storage::{Store, UnixMillis};
