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

pub mod coordinator;
pub mod multikey;
pub mod pubsub;
pub mod raft_boot;
pub mod serve;
pub mod spanning_combine;
pub mod test_support;
pub mod whole_keyspace;
