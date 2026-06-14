// SPDX-License-Identifier: MIT OR Apache-2.0
//! The [`Value`] type: the full RESP3 type space the serializer emits.
//!
//! A reply is built as a `Value` and then encoded for the connection's negotiated
//! protocol (RESP2 or RESP3) by [`crate::encode::encode`]. Under RESP2 the RESP3
//! aggregate and scalar types degrade to their RESP2 equivalents per ADR-0019;
//! that degradation lives in the encoder, not here, so callers build one reply
//! and the proto decides the bytes.

use crate::error::ErrorReply;
use bytes::Bytes;

/// The RESP protocol version negotiated for a connection. A connection starts in
/// [`ProtoVersion::Resp2`] and switches only via `HELLO 3` (PROTOCOL.md,
/// ADR-0019).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtoVersion {
    /// RESP2 (the default before `HELLO 3`).
    #[default]
    Resp2,
    /// RESP3 (opt-in via `HELLO 3`).
    Resp3,
}

impl ProtoVersion {
    /// The integer form used in `HELLO` replies and negotiation (`2` or `3`).
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        match self {
            ProtoVersion::Resp2 => 2,
            ProtoVersion::Resp3 => 3,
        }
    }
}

/// A RESP value. Covers the full RESP3 type set; the encoder maps each to RESP2
/// where the connection has not upgraded (ADR-0019).
///
/// This is the reply-side type. The request side is parsed into a
/// [`crate::decode::Request`] (always an array of bulk strings or an inline
/// command), not into `Value`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `+OK\r\n` style simple string.
    SimpleString(String),
    /// An error reply from the catalog (rendered `-<TOKEN> msg\r\n`).
    Error(ErrorReply),
    /// `:<n>\r\n` 64-bit signed integer.
    Integer(i64),
    /// A bulk string, or the null bulk string when `None`.
    BulkString(Option<Bytes>),
    /// An array, or the null array when `None`.
    Array(Option<Vec<Value>>),
    /// The RESP3 null (`_\r\n`); degrades to `$-1`/`*-1` under RESP2 (ADR-0019).
    Null,
    /// RESP3 double (`,`); degrades to a bulk string under RESP2.
    Double(f64),
    /// RESP3 boolean (`#t`/`#f`); degrades to `:1`/`:0` under RESP2.
    Boolean(bool),
    /// RESP3 big number (`(`); degrades to a bulk string under RESP2.
    BigNumber(String),
    /// RESP3 bulk error (`!`); degrades to a simple error under RESP2.
    BulkError(ErrorReply),
    /// RESP3 verbatim string (`=`) with a 3-char format (e.g. `txt`, `mkd`);
    /// degrades to a bulk string under RESP2.
    VerbatimString { format: [u8; 3], data: Bytes },
    /// RESP3 map (`%`); degrades to a flat array under RESP2.
    Map(Vec<(Value, Value)>),
    /// RESP3 set (`~`); degrades to an array under RESP2.
    Set(Vec<Value>),
    /// RESP3 push (`>`); degrades to an array under RESP2.
    Push(Vec<Value>),
}

impl Value {
    /// A `+OK` simple string, the most common success reply.
    #[must_use]
    pub fn ok() -> Value {
        Value::SimpleString("OK".to_owned())
    }

    /// A bulk string from anything byte-like.
    #[must_use]
    pub fn bulk(data: impl Into<Bytes>) -> Value {
        Value::BulkString(Some(data.into()))
    }

    /// A bulk string from a `&str`.
    #[must_use]
    pub fn bulk_str(s: &str) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())))
    }

    /// A simple string from a `&str`.
    #[must_use]
    pub fn simple(s: &str) -> Value {
        Value::SimpleString(s.to_owned())
    }

    /// An error value from a catalog [`ErrorReply`].
    #[must_use]
    pub fn error(e: ErrorReply) -> Value {
        Value::Error(e)
    }
}
