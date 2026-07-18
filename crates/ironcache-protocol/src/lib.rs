// SPDX-License-Identifier: MIT OR Apache-2.0
//! RESP2/RESP3 protocol surface for IronCache (PROTOCOL.md, ERRORS.md, ADR-0019).
//!
//! Three freeze-point pieces:
//!
//! - [`value::Value`] - the reply type spanning the RESP3 type space, plus
//!   [`value::ProtoVersion`].
//! - [`decode::decode`] - the incremental request decoder returning
//!   [`decode::DecodeOutcome`] (`Incomplete` / `Complete` / `Error`).
//! - [`encode::encode`] - the version-parameterized serializer (RESP2/RESP3
//!   reply shaping per ADR-0019).
//! - [`error::ErrorCode`] / [`error::ErrorReply`] - the canonical error catalog
//!   with verbatim handshake-critical strings.
//!
//! The crate is `no`-OS-time and `no`-rand by construction (it is a pure codec),
//! so it satisfies the determinism invariant trivially.

#![forbid(unsafe_code)]

pub mod decode;
pub mod encode;
pub mod error;
pub mod slot;
pub mod value;

pub use decode::{DecodeOutcome, Limits, Request, decode, decode_shared};
pub use encode::{
    encode, encode_bulk_len_prefix, encode_bulk_ref, encode_to_vec, format_human_double,
};
pub use error::{ErrorCode, ErrorReply};
pub use slot::{CLUSTER_SLOTS, crc16, hash_tag, key_slot};
pub use value::{ProtoVersion, Value};
