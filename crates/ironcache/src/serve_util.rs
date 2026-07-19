// SPDX-License-Identifier: MIT OR Apache-2.0
//! Ubiquitous serve-path leaf helpers split out of `serve.rs` (#625): the reply-encoder shim and the
//! routing-token uppercaser. Both are called from nearly every serve submodule (`encode_into` from
//! ~28 sites, `ascii_upper` from ~10 plus `crate::coordinator`) and depend on nothing else in
//! `serve`, so they live together in one leaf module. Behavior-preserving relocation: the fn bodies
//! are byte-identical to their former in-`serve.rs` definitions.

use ironcache_server::ProtoVersion;

/// Encode `value` and append the bytes to `out`. `Vec<u8>` is a `bytes::BufMut` sink, so
/// `encode` writes the reply STRAIGHT into `out` -- no per-reply `BytesMut` allocation and no
/// intermediate copy (the encoder is generic over the sink; PROTOCOL.md's zero-copy note).
pub(crate) fn encode_into(out: &mut Vec<u8>, value: &ironcache_server::Value, proto: ProtoVersion) {
    ironcache_protocol::encode(out, value, proto);
}

/// ASCII-uppercase the command token for routing classification (RESP command tokens are
/// ASCII; mirrors the dispatcher's own case-insensitive token handling). The classified
/// token is used ONLY to pick a route; dispatch re-uppercases its own copy. `pub(crate)`
/// so the [`crate::coordinator`] drain loop classifies the same way (keyed vs whole-keyspace).
///
/// Delegates to the canonical [`ironcache_server::cmd_util::ascii_upper`], whose stack-backed
/// `UpperToken` classifies the per-command token with NO heap allocation on this hot path.
pub(crate) fn ascii_upper(b: &[u8]) -> ironcache_server::cmd_util::UpperToken {
    ironcache_server::cmd_util::ascii_upper(b)
}
