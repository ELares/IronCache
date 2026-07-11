// SPDX-License-Identifier: MIT OR Apache-2.0
//! `DUMP` / `RESTORE` (#129, KEYSPACE.md): the Redis-compatible value serialization blob.
//!
//! `DUMP key` emits an opaque byte string; `RESTORE key ttl <blob>` recreates the value from one.
//! The blob is the Redis/Valkey `DUMP` format so it INTEROPERATES with a real `redis-server` (the
//! differential oracle, #97): a blob IronCache emits, redis RESTOREs to the same value, and vice
//! versa. The blob is `<type><rdb-encoded-value> || rdb_version[2 LE] || crc64[8 LE]`, where the
//! CRC-64 (Jones variant, the Redis polynomial) covers the value bytes AND the version, and RESTORE
//! rejects a version newer than [`SUPPORTED_RDB_VERSION`] or a bad checksum, exactly as
//! `verifyDumpPayload` does.
//!
//! ## Scope
//!
//! DUMP (encode) emits the STRING type (`RDB_TYPE_STRING = 0`) only. Because a HyperLogLog is stored
//! AS a string (`cmd_hll`), this gives HLL DUMP/RESTORE byte-interop for free (#242 part 2: an HLL
//! DUMPed here RESTOREs + PFCOUNTs identically on redis). DUMP of a non-string value is still a typed
//! "unsupported" error rather than a wrong blob.
//!
//! RESTORE (decode) additionally accepts the SET type in all three RDB encodings -- the plain
//! `RDB_TYPE_SET`, the `RDB_TYPE_SET_INTSET`, and the `RDB_TYPE_SET_LISTPACK` -- the HASH type in
//! its two non-field-TTL RDB encodings -- the plain `RDB_TYPE_HASH` and the `RDB_TYPE_HASH_LISTPACK`
//! -- and the ZSET type in all three RDB encodings -- `RDB_TYPE_ZSET_2` (8-byte little-endian
//! binary-double scores), the legacy `RDB_TYPE_ZSET` (length-prefixed ASCII scores with the
//! -inf/+inf/NaN sentinels), and `RDB_TYPE_ZSET_LISTPACK` -- so a set, a (non-field-TTL) hash, OR a
//! sorted set DUMPed by a real redis RESTOREs here with identical members/fields/scores (#612 phase;
//! DUMP of an aggregate stays deferred). A NaN score is refused as bad data in every zset encoding
//! (parity with our ZADD guard + Redis's post-load isnan check); a +inf/-inf score is a legitimate
//! value and is preserved. The field-TTL hash encodings (`RDB_TYPE_HASH_LISTPACK_EX`,
//! `RDB_TYPE_HASH_METADATA`, and their pre-GA forms) are a tracked follow-up (#612 PR4) and are
//! refused as bad data for now rather than half-decoded (a field TTL is never silently dropped). The
//! remaining aggregate type (list) RESTORE is the tracked follow-up; a type we do not yet decode is
//! refused as bad data, never mis-decoded.
//!
//! The reusable RDB codec substrate (the CRC-64 footer, the RDB length + string encodings, and the
//! container element iterators the aggregate types build on) lives in [`crate::rdb`]; this slice is
//! the STRING encode/decode plus the command surface.
//!
//! ## Encoding fidelity
//!
//! DUMP writes the RAW string length-encoding (no LZF compression, no integer encoding): always a
//! VALID Redis payload that redis RESTOREs, just not byte-identical to what redis's own DUMP would
//! emit for a compressible/integer value (redis may LZF- or int-encode). RESTORE, conversely, ACCEPTS
//! all of redis's encodings -- raw, the `INT8`/`INT16`/`INT32` special encodings, and LZF-compressed
//! strings -- so a redis-produced blob always loads. The CRC-64 and the RDB length codec are
//! validated against published known-answer vectors; the round trip + the redis interop are the
//! oracle (differential.rs).

use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

use crate::rdb::{
    DECODE_PREALLOC_CAP, DUMP_RDB_VERSION, LpElem, RDB_TYPE_HASH, RDB_TYPE_HASH_LISTPACK,
    RDB_TYPE_HASH_LISTPACK_EX, RDB_TYPE_HASH_LISTPACK_EX_PRE_GA, RDB_TYPE_HASH_METADATA,
    RDB_TYPE_HASH_METADATA_PRE_GA, RDB_TYPE_SET, RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK,
    RDB_TYPE_STRING, RDB_TYPE_ZSET, RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK, RestoreParseError,
    crc64, intset_iter, listpack_iter, parse_ascii_double, read_rdb_ascii_double,
    read_rdb_binary_double, read_rdb_len, read_rdb_string, verify_footer, write_rdb_len,
};

/// A decoded hash's `(field, value)` pairs in stream order (a repeated field is resolved last-wins by
/// [`NewValueOwned::Hash`]). Aliased to keep the [`deserialize_hash`] signature under the
/// `type_complexity` lint, mirroring `cmd_hash`'s `FieldValue`.
type HashPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// A decoded zset's `(member, score)` pairs in stream order (a repeated member is resolved
/// last-score-wins by [`NewValueOwned::ZSet`] / `ZSetVal::from_pairs`). Aliased to keep the
/// [`deserialize_zset`] signature under the `type_complexity` lint, mirroring [`HashPairs`].
type ZSetPairs = Vec<(Vec<u8>, f64)>;

/// Serialize `value` (its raw bytes) as a full DUMP blob: `type || raw-string || version || crc64`.
#[must_use]
pub fn serialize_string(value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(value.len() + 16);
    payload.push(RDB_TYPE_STRING);
    write_rdb_len(&mut payload, value.len() as u64);
    payload.extend_from_slice(value);
    payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
    let crc = crc64(0, &payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

/// Parse a DUMP blob (footer-verified) into the STRING value bytes. Rejects a non-string type or
/// trailing garbage after the value.
fn deserialize_string(blob: &[u8]) -> Result<Vec<u8>, RestoreParseError> {
    let payload = verify_footer(blob)?;
    let mut pos = 0usize;
    let ty = *payload.get(pos).ok_or(RestoreParseError::BadData)?;
    pos += 1;
    if ty != RDB_TYPE_STRING {
        // Other RDB types (list/set/hash/zset/...) are a tracked follow-up; a valid-but-unsupported
        // type is BadData here (the value cannot be reconstructed), not a checksum error.
        return Err(RestoreParseError::BadData);
    }
    let value = read_rdb_string(payload, &mut pos)?;
    if pos != payload.len() {
        return Err(RestoreParseError::BadData); // trailing bytes: malformed
    }
    Ok(value)
}

/// Parse a DUMP blob (footer-verified) into the SET members, ready for [`NewValueOwned::Set`] (which
/// dedups + applies the intset/listpack/hashtable ladder). Handles the three RDB set encodings:
///
/// * `RDB_TYPE_SET` (2): an RDB length = member count, then that many RDB strings. The member count
///   is bounded against the remaining payload bytes BEFORE any pre-allocation (a member is at least
///   one byte, so a count past the remaining bytes is a hostile/garbage header), and each member
///   goes through [`read_rdb_string`] (inheriting its LZF + length-gating + bounded-alloc discipline).
/// * `RDB_TYPE_SET_INTSET` (11): the intset blob, itself stored AS an RDB string (so redis may LZF- or
///   raw-encode it), decoded by [`intset_iter`]; each integer materializes as its DECIMAL ASCII text
///   (redis renders a materialized intset member with `ll2string`, e.g. `-5` -> `"-5"`).
/// * `RDB_TYPE_SET_LISTPACK` (20): the listpack blob, likewise stored AS an RDB string, decoded by
///   [`listpack_iter`]; an [`LpElem::Int`] renders as decimal ASCII and an [`LpElem::Str`] is the raw
///   bytes.
///
/// Every declared length is bounds-checked before a slice or allocation (the shared `rdb` discipline),
/// so a hostile blob is a clean [`RestoreParseError::BadData`], never a panic or an over-allocation.
fn deserialize_set(blob: &[u8]) -> Result<Vec<Vec<u8>>, RestoreParseError> {
    let payload = verify_footer(blob)?;
    let mut pos = 0usize;
    let ty = *payload.get(pos).ok_or(RestoreParseError::BadData)?;
    pos += 1;
    match ty {
        RDB_TYPE_SET => {
            let (count, is_encoded) = read_rdb_len(payload, &mut pos)?;
            if is_encoded {
                // A member count is never one of the RDB_ENCVAL special encodings.
                return Err(RestoreParseError::BadData);
            }
            let count = usize::try_from(count).map_err(|_| RestoreParseError::BadData)?;
            // Bound the declared count against the bytes still available: the smallest member is a
            // single length byte (a zero-length string), so a count larger than the remaining bytes
            // is a lie -> BadData BEFORE the pre-allocation. `read_rdb_string` re-validates each
            // member's own declared length.
            if count > payload.len().saturating_sub(pos) {
                return Err(RestoreParseError::BadData);
            }
            let mut members = Vec::with_capacity(count.min(DECODE_PREALLOC_CAP));
            for _ in 0..count {
                members.push(read_rdb_string(payload, &mut pos)?);
            }
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes: malformed
            }
            Ok(members)
        }
        RDB_TYPE_SET_INTSET => {
            // The intset blob is stored AS an RDB string (redis `rdbSaveRawString`), so decode the
            // string first (LZF handled for free), then the intset, then render each integer as its
            // decimal text so a RESTOREd intset yields string members "1", "2", ... .
            let body = read_rdb_string(payload, &mut pos)?;
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes after the intset string
            }
            let ints = intset_iter(&body)?;
            Ok(ints
                .into_iter()
                .map(|n| n.to_string().into_bytes())
                .collect())
        }
        RDB_TYPE_SET_LISTPACK => {
            // Likewise stored AS an RDB string; each listpack element is a member (an int renders as
            // decimal ASCII, a string is the raw bytes).
            let body = read_rdb_string(payload, &mut pos)?;
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes after the listpack string
            }
            let elems = listpack_iter(&body)?;
            Ok(elems
                .into_iter()
                .map(|e| match e {
                    LpElem::Int(n) => n.to_string().into_bytes(),
                    LpElem::Str(b) => b.into_vec(),
                })
                .collect())
        }
        // A non-set type reaching here is a routing bug; caller dispatches STRING elsewhere and any
        // other type is a tracked follow-up, refused as BadData (the value cannot be reconstructed).
        _ => Err(RestoreParseError::BadData),
    }
}

/// Parse a DUMP blob (footer-verified) into the HASH `(field, value)` pairs, ready for
/// [`NewValueOwned::Hash`] (which applies the listpack/hashtable ladder and, via `HashVal::set`, keeps
/// the LAST value for a repeated field -- Redis's last-wins semantics -- so the decoder need not
/// pre-dedup). Handles the two NON-field-TTL RDB hash encodings:
///
/// * `RDB_TYPE_HASH` (4): an RDB length = the field/value PAIR count, then `2*count` RDB strings read
///   alternately field, value, field, value. The implied `2*count` string count is bounded against the
///   remaining payload bytes BEFORE any pre-allocation (a string is at least one length byte, so a pair
///   count implying more strings than remaining bytes is a hostile/garbage header; `checked_mul` guards
///   the doubling), and each string goes through [`read_rdb_string`] (inheriting its LZF + length-gating
///   + bounded-alloc discipline).
/// * `RDB_TYPE_HASH_LISTPACK` (16): the listpack blob, itself stored AS an RDB string (so redis may LZF-
///   or raw-encode it, EXACTLY like the SET_LISTPACK case), decoded by [`listpack_iter`]; its elements
///   are the FLATTENED pairs `[field, value, field, value, ...]`, so the element count MUST be EVEN --
///   an odd count is a corrupt/hostile blob and is [`RestoreParseError::BadData`]. An [`LpElem::Int`]
///   field OR value renders as its DECIMAL ASCII text (`ll2string`, as for a set) and an [`LpElem::Str`]
///   is the raw bytes.
///
/// The field-TTL hash encodings -- `RDB_TYPE_HASH_LISTPACK_EX` (25), `RDB_TYPE_HASH_METADATA` (24), and
/// their 7.4 pre-GA forms (23 / 22) -- are a tracked follow-up (#612 PR4). We do NOT half-decode a field
/// TTL, so they are refused as [`RestoreParseError::BadData`] here, never mis-decoded (a field TTL is
/// never silently dropped).
///
/// Every declared length is bounds-checked before a slice or allocation (the shared `rdb` discipline),
/// so a hostile blob is a clean [`RestoreParseError::BadData`], never a panic or an over-allocation.
fn deserialize_hash(blob: &[u8]) -> Result<HashPairs, RestoreParseError> {
    let payload = verify_footer(blob)?;
    let mut pos = 0usize;
    let ty = *payload.get(pos).ok_or(RestoreParseError::BadData)?;
    pos += 1;
    match ty {
        RDB_TYPE_HASH => {
            let (count, is_encoded) = read_rdb_len(payload, &mut pos)?;
            if is_encoded {
                // A field/value pair count is never one of the RDB_ENCVAL special encodings.
                return Err(RestoreParseError::BadData);
            }
            let count = usize::try_from(count).map_err(|_| RestoreParseError::BadData)?;
            // Each pair is TWO RDB strings, so the stream must hold `2*count` strings; the smallest
            // string is a single length byte, so `2*count` larger than the remaining bytes is a lie ->
            // BadData BEFORE the pre-allocation. `checked_mul` guards the doubling from overflow, and
            // `read_rdb_string` re-validates each string's own declared length.
            let strings = count.checked_mul(2).ok_or(RestoreParseError::BadData)?;
            if strings > payload.len().saturating_sub(pos) {
                return Err(RestoreParseError::BadData);
            }
            let mut pairs = Vec::with_capacity(count.min(DECODE_PREALLOC_CAP));
            for _ in 0..count {
                let field = read_rdb_string(payload, &mut pos)?;
                let value = read_rdb_string(payload, &mut pos)?;
                pairs.push((field, value));
            }
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes: malformed
            }
            Ok(pairs)
        }
        RDB_TYPE_HASH_LISTPACK => {
            // Stored AS an RDB string (redis `rdbSaveRawString`); decode the string first (LZF handled
            // for free), then the listpack, whose elements are the flattened field/value pairs.
            let body = read_rdb_string(payload, &mut pos)?;
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes after the listpack string
            }
            let elems = listpack_iter(&body)?;
            // A hash listpack is field/value PAIRS, so the element count must be EVEN; an odd count is
            // a corrupt/hostile blob (redis never writes one).
            if elems.len() % 2 != 0 {
                return Err(RestoreParseError::BadData);
            }
            let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(elems.len() / 2);
            let mut it = elems.into_iter();
            // The even-count check above guarantees `it` yields pairs; the `while let` pattern stops
            // cleanly at exhaustion regardless. An int element renders as decimal ASCII, a string is
            // the raw bytes (matching the SET_LISTPACK rendering).
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                let field = match f {
                    LpElem::Int(n) => n.to_string().into_bytes(),
                    LpElem::Str(b) => b.into_vec(),
                };
                let value = match v {
                    LpElem::Int(n) => n.to_string().into_bytes(),
                    LpElem::Str(b) => b.into_vec(),
                };
                pairs.push((field, value));
            }
            Ok(pairs)
        }
        // The field-TTL hash encodings (`RDB_TYPE_HASH_LISTPACK_EX` / `_METADATA` and their 7.4 pre-GA
        // forms 23 / 22) are a tracked follow-up (#612 PR4): we do NOT decode a field TTL, so they are
        // refused as BadData rather than mis-decoded (a field TTL is never silently dropped). Any OTHER
        // type reaching here is a routing bug (the caller dispatches the decodable types elsewhere);
        // it is likewise BadData, since the value cannot be reconstructed. The two cases share a body,
        // so they collapse into one wildcard arm.
        _ => Err(RestoreParseError::BadData),
    }
}

/// Parse a DUMP blob (footer-verified) into the ZSET `(member, score)` pairs, ready for
/// [`NewValueOwned::ZSet`] (which applies the listpack/skiplist ladder + the (score, member) ordering
/// and, via `ZSetVal::from_pairs`, keeps the LAST score for a repeated member -- Redis's last-wins
/// semantics -- so the decoder need not pre-dedup). A NaN score is rejected as
/// [`RestoreParseError::BadData`] in ONE place after each score is read, exactly as Redis rejects a
/// NaN-scored zset element on load (`rdbReportCorruptRDB("Zset with NAN score detected")`) and our own
/// ZADD refuses a NaN; a +inf/-inf score is a LEGITIMATE value and is preserved. Handles the three RDB
/// sorted-set encodings:
///
/// * `RDB_TYPE_ZSET_2` (5): an RDB length = the member count, then `count` x (an [`read_rdb_string`]
///   member + an 8-byte little-endian IEEE754 `binary64` score, [`read_rdb_binary_double`]). The count
///   is bounded against the remaining bytes BEFORE any pre-allocation: each element is at least a
///   1-byte member length + 8 score bytes = 9 bytes, so a `count * 9` larger than the remaining bytes
///   is a hostile/garbage header (`checked_mul` guards the product).
/// * `RDB_TYPE_ZSET` (3, legacy ASCII scores): an RDB length = the member count, then `count` x (a
///   member + a length-prefixed ASCII score with the `255`/`254`/`253` = -inf/+inf/NaN sentinels,
///   [`read_rdb_ascii_double`]). Each element is at least a 1-byte member length + a 1-byte score
///   length = 2 bytes, bounding the count as above.
/// * `RDB_TYPE_ZSET_LISTPACK` (17): the listpack blob, itself stored AS an RDB string (so redis may
///   LZF- or raw-encode it, EXACTLY like the SET_LISTPACK / HASH_LISTPACK cases), decoded by
///   [`listpack_iter`]; its elements are the FLATTENED `[member, score, member, score, ...]`, so the
///   count MUST be EVEN (an odd count is a corrupt/hostile blob -> BadData). An [`LpElem::Int`] member
///   renders as its DECIMAL ASCII text (`ll2string`, as for a set/hash) and an [`LpElem::Str`] is the
///   raw bytes; a score that is an [`LpElem::Int`] is that integer as an `f64`, a score that is an
///   [`LpElem::Str`] is the ASCII float text parsed by [`parse_ascii_double`].
///
/// Every declared length is bounds-checked before a slice or allocation (the shared `rdb` discipline),
/// so a hostile blob is a clean [`RestoreParseError::BadData`], never a panic or an over-allocation.
fn deserialize_zset(blob: &[u8]) -> Result<ZSetPairs, RestoreParseError> {
    let payload = verify_footer(blob)?;
    let mut pos = 0usize;
    let ty = *payload.get(pos).ok_or(RestoreParseError::BadData)?;
    pos += 1;
    match ty {
        RDB_TYPE_ZSET_2 | RDB_TYPE_ZSET => {
            let (count, is_encoded) = read_rdb_len(payload, &mut pos)?;
            if is_encoded {
                // A member count is never one of the RDB_ENCVAL special encodings.
                return Err(RestoreParseError::BadData);
            }
            let count = usize::try_from(count).map_err(|_| RestoreParseError::BadData)?;
            // Bound the declared count against the bytes still available BEFORE the pre-allocation: an
            // element is a member (>= 1 length byte) plus a score (8 bytes for ZSET_2's binary double,
            // >= 1 byte for ZSET's length-prefixed ASCII score / sentinel). A count implying more
            // minimum-size elements than remaining bytes is a lie -> BadData. `checked_mul` guards the
            // product, and `read_rdb_string` / the double readers re-validate each element's own bytes.
            let per_elem = if ty == RDB_TYPE_ZSET_2 { 9 } else { 2 };
            let min_bytes = count
                .checked_mul(per_elem)
                .ok_or(RestoreParseError::BadData)?;
            if min_bytes > payload.len().saturating_sub(pos) {
                return Err(RestoreParseError::BadData);
            }
            let mut pairs = Vec::with_capacity(count.min(DECODE_PREALLOC_CAP));
            for _ in 0..count {
                let member = read_rdb_string(payload, &mut pos)?;
                let score = if ty == RDB_TYPE_ZSET_2 {
                    read_rdb_binary_double(payload, &mut pos)?
                } else {
                    read_rdb_ascii_double(payload, &mut pos)?
                };
                // A NaN score (a NaN bit pattern in the binary form, the 253 sentinel or "nan" text in
                // the ASCII form) is corrupt: reject it, matching Redis's post-load isnan guard and our
                // ZADD. A +inf/-inf is finite-enough to keep.
                if score.is_nan() {
                    return Err(RestoreParseError::BadData);
                }
                pairs.push((member, score));
            }
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes: malformed
            }
            Ok(pairs)
        }
        RDB_TYPE_ZSET_LISTPACK => {
            // Stored AS an RDB string (redis `rdbSaveRawString`); decode the string first (LZF handled
            // for free), then the listpack, whose elements are the flattened member/score pairs.
            let body = read_rdb_string(payload, &mut pos)?;
            if pos != payload.len() {
                return Err(RestoreParseError::BadData); // trailing bytes after the listpack string
            }
            let elems = listpack_iter(&body)?;
            // A zset listpack is member/score PAIRS, so the element count must be EVEN; an odd count is
            // a corrupt/hostile blob (redis never writes one).
            if elems.len() % 2 != 0 {
                return Err(RestoreParseError::BadData);
            }
            let mut pairs: ZSetPairs = Vec::with_capacity(elems.len() / 2);
            let mut it = elems.into_iter();
            // The even-count check above guarantees `it` yields pairs; the `while let` stops cleanly at
            // exhaustion. A member int renders as decimal ASCII, a member string is the raw bytes
            // (matching SET_LISTPACK / HASH_LISTPACK); a score int IS that integer as an f64, a score
            // string is its ASCII float text.
            while let (Some(m), Some(sc)) = (it.next(), it.next()) {
                let member = match m {
                    LpElem::Int(n) => n.to_string().into_bytes(),
                    LpElem::Str(b) => b.into_vec(),
                };
                let score = match sc {
                    // A listpack int score is that integer as an f64 (the workspace allows the
                    // cast_precision_loss: a real score is a whole number well within f64's exact range).
                    LpElem::Int(n) => n as f64,
                    LpElem::Str(b) => parse_ascii_double(&b)?,
                };
                if score.is_nan() {
                    return Err(RestoreParseError::BadData);
                }
                pairs.push((member, score));
            }
            Ok(pairs)
        }
        // A non-zset type reaching here is a routing bug; the caller dispatches the decodable types
        // elsewhere and any other type is refused as BadData (the value cannot be reconstructed).
        _ => Err(RestoreParseError::BadData),
    }
}

// ---------------------------------------------------------------------------
// The commands.
// ---------------------------------------------------------------------------

/// `DUMP key` -> the serialized value as a bulk string, or the null bulk string for a missing key.
/// WRONGTYPE-style unsupported for a non-string value type (only the STRING type is serialized in
/// this slice; an HLL is a string, so it works). READ-ONLY.
pub fn cmd_dump<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 2 {
        return Value::error(ErrorReply::wrong_arity("dump"));
    }
    match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => {
            Value::BulkString(Some(Bytes::from(serialize_string(v.as_bytes()))))
        }
        Some(_) => Value::error(ErrorReply::err(
            "DUMP of this value type is not yet supported (string only)",
        )),
        None => Value::Null,
    }
}

/// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ freq]` -> `+OK`.
/// Recreates the value from a DUMP blob. `ttl` is milliseconds (0 = no expiry; `ABSTTL` = an absolute
/// unix-ms deadline). Without `REPLACE`, an existing key is a `BUSYKEY` error. `IDLETIME`/`FREQ` are
/// accepted and ignored (LRU/LFU hints do not affect the value). `denyoom`.
pub fn cmd_restore<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity("restore"));
    }
    // Parse ttl + the option flags.
    let Some(ttl_ms) = parse_i64(&req.args[2]) else {
        return Value::error(ErrorReply::not_an_integer());
    };
    if ttl_ms < 0 {
        return Value::error(ErrorReply::restore_invalid_ttl());
    }
    let mut replace = false;
    let mut absttl = false;
    let mut i = 4;
    while i < req.args.len() {
        match crate::cmd_util::ascii_upper(&req.args[i]).as_slice() {
            b"REPLACE" => {
                replace = true;
                i += 1;
            }
            b"ABSTTL" => {
                absttl = true;
                i += 1;
            }
            // IDLETIME/FREQ carry an LRU/LFU hint that does not change the value, but redis still
            // RANGE-validates them (and errors on a non-integer as NOT-an-integer, not a syntax error).
            b"IDLETIME" => match req.args.get(i + 1).and_then(|a| parse_i64(a)) {
                None if req.args.len() <= i + 1 => return Value::error(ErrorReply::syntax_error()),
                None => return Value::error(ErrorReply::not_an_integer()),
                Some(v) if v < 0 => {
                    return Value::error(ErrorReply::err("Invalid IDLETIME value, must be >= 0"));
                }
                Some(_) => i += 2,
            },
            b"FREQ" => match req.args.get(i + 1).and_then(|a| parse_i64(a)) {
                None if req.args.len() <= i + 1 => return Value::error(ErrorReply::syntax_error()),
                None => return Value::error(ErrorReply::not_an_integer()),
                Some(v) if !(0..=255).contains(&v) => {
                    return Value::error(ErrorReply::err(
                        "Invalid FREQ value, must be >= 0 and <= 255",
                    ));
                }
                Some(_) => i += 2,
            },
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    }

    // Compute the deadline: 0 -> no expiry; ABSTTL -> the value as-is; else now + ttl.
    // ttl_ms is validated >= 0 above, so the u64 cast is lossless.
    let ttl = ttl_ms as u64;
    let expire = if ttl_ms == 0 {
        ExpireWrite::Clear
    } else if absttl {
        ExpireWrite::Set(UnixMillis(ttl))
    } else {
        ExpireWrite::Set(UnixMillis(now.0.saturating_add(ttl)))
    };

    // The decode happens INSIDE the rmw closure so redis's error PRECEDENCE holds: an existing key
    // without REPLACE is BUSYKEY *before* the payload is even parsed. A bad payload on the write path
    // returns Keep (no mutation) -- still fail-closed.
    let blob = &req.args[3];
    store.rmw(db, &req.args[1], now, move |entry| {
        if matches!(entry, RmwEntry::Occupied(_)) && !replace {
            return RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: Value::error(ErrorReply::busykey_target_exists()),
            };
        }
        // Route on the RDB type byte, which is `blob[0]` (the first payload byte, before the footer)
        // and is CRC-covered, so `verify_footer` inside the chosen decoder still authenticates it: a
        // SET type reconstructs a `NewValueOwned::Set`, a HASH type a `NewValueOwned::Hash`, and a ZSET
        // type a `NewValueOwned::ZSet` (the store dedups + applies the encoding ladder), everything
        // else falls to the STRING decoder (which rejects a non-STRING type as BadData). The four
        // field-TTL hash type bytes route to `deserialize_hash` too, which cleanly refuses them as
        // BadData (PR4), so no key is created. All paths install through the same `RmwAction::Replace`
        // used by the string RESTORE -- only the value construction differs, so REPLACE / ttl / ABSTTL
        // / IDLETIME / FREQ all hold.
        let decoded = match blob.first() {
            Some(&RDB_TYPE_SET | &RDB_TYPE_SET_INTSET | &RDB_TYPE_SET_LISTPACK) => {
                deserialize_set(blob).map(NewValueOwned::Set)
            }
            Some(
                &RDB_TYPE_HASH
                | &RDB_TYPE_HASH_LISTPACK
                | &RDB_TYPE_HASH_LISTPACK_EX
                | &RDB_TYPE_HASH_METADATA
                | &RDB_TYPE_HASH_METADATA_PRE_GA
                | &RDB_TYPE_HASH_LISTPACK_EX_PRE_GA,
            ) => deserialize_hash(blob).map(NewValueOwned::Hash),
            Some(&RDB_TYPE_ZSET_2 | &RDB_TYPE_ZSET | &RDB_TYPE_ZSET_LISTPACK) => {
                deserialize_zset(blob).map(NewValueOwned::ZSet)
            }
            _ => deserialize_string(blob).map(|v| NewValueOwned::Bytes(Bytes::from(v))),
        };
        match decoded {
            // Vacant, or Occupied with REPLACE: write the value (Replace on a vacant entry inserts).
            Ok(value) => RmwStep {
                action: RmwAction::Replace(value),
                expire,
                reply: Value::ok(),
            },
            Err(e) => RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: Value::error(e.into_reply()),
            },
        }
    })
}

/// Parse a base-10 signed integer argument (RESTORE ttl / IDLETIME / FREQ) with the STRICTNESS of
/// redis's `string2ll`: no leading `+`, no surrounding whitespace, no leading zeros (only the
/// standalone `0`), no `-0`. This keeps RESTORE's argument acceptance byte-for-byte with redis, so a
/// value redis rejects (`RESTORE k +5 ...`, `RESTORE k 007 ...`) is rejected here too.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let magnitude = s.strip_prefix('-').unwrap_or(s);
    // All digits, non-empty (this also rejects a leading '+', whitespace, or any sign but '-').
    if magnitude.is_empty() || !magnitude.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Leading zeros: only the standalone "0" is legal; "007" / "-0" are not (redis string2ll).
    if (magnitude.len() > 1 && magnitude.starts_with('0'))
        || (s.starts_with('-') && magnitude == "0")
    {
        return None;
    }
    // `s` now has at most a leading '-' and canonical digits, so std parse (which handles i64::MIN)
    // agrees with redis; the '+' std would otherwise accept was already excluded above.
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdb::SUPPORTED_RDB_VERSION;
    use ironcache_storage::{CountingAccounting, Store};
    use ironcache_store::ShardStore;

    type TestStore = ShardStore<ironcache_eviction::Policy, CountingAccounting>;
    const NOW: UnixMillis = UnixMillis(1_000_000);

    fn test_store() -> TestStore {
        ShardStore::with_hooks(
            1,
            ironcache_eviction::Policy::cache_default(),
            CountingAccounting::new(),
        )
    }
    fn req(parts: &[&[u8]]) -> Request {
        Request {
            args: parts.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        }
    }

    /// Seed a raw string value at `key` (avoids the SET command's `TimingWheel` dependency).
    fn seed(store: &mut TestStore, key: &[u8], val: &[u8]) {
        use ironcache_storage::{ExpireWrite, NewValue};
        store.upsert(0, key, NewValue::Bytes(val), ExpireWrite::Clear, NOW);
    }

    #[test]
    fn string_round_trips_through_serialize_deserialize() {
        for v in [
            &b""[..],
            b"hello",
            b"12345",                      // numeric, but we raw-encode on DUMP
            &vec![0u8, 159, 146, 150][..], // invalid utf-8 bytes
            &vec![b'x'; 300][..],          // crosses the 14-bit length class
        ] {
            let blob = serialize_string(v);
            assert_eq!(
                deserialize_string(&blob).unwrap(),
                v,
                "round trip for {v:?}"
            );
        }
    }

    #[test]
    fn deserialize_decodes_redis_int_and_lzf_encodings() {
        // A hand-built RDB int8 blob for the value "42": type 0, RDB_ENCVAL|INT8 (0xC0), byte 42.
        let mut payload = vec![RDB_TYPE_STRING, 0xC0, 42u8];
        payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(deserialize_string(&payload).unwrap(), b"42");

        // A hand-built LZF blob for "aaaaaaaa" (8 'a'): a 1-byte literal 'a' then a back-reference.
        // Compressed stream: [0x00, 'a']  (literal run of 1: ctrl 0 -> 1 byte)
        //                    [0xE0, 0x00] (backref: len bits = 7? no) -> build len=5 copies dist=0.
        // ctrl for backref: (len-2)<<5 | (dist>>8); here len=7 (=> +2 = ... ). Use len field=5: (5)<<5=0xA0
        //   with dist-1 = 0 -> ctrl 0xA0, next byte 0x00 -> copies 5+2=7 bytes from distance 0+1=1.
        let lzf_stream = [0x00u8, b'a', 0xA0, 0x00];
        let mut lzf_payload = vec![RDB_TYPE_STRING, 0xC3]; // RDB_ENCVAL|LZF
        write_rdb_len(&mut lzf_payload, lzf_stream.len() as u64); // clen
        write_rdb_len(&mut lzf_payload, 8); // ulen
        lzf_payload.extend_from_slice(&lzf_stream);
        lzf_payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
        let crc2 = crc64(0, &lzf_payload);
        lzf_payload.extend_from_slice(&crc2.to_le_bytes());
        assert_eq!(deserialize_string(&lzf_payload).unwrap(), b"aaaaaaaa");
    }

    #[test]
    fn restore_rejects_a_corrupted_checksum() {
        let mut blob = serialize_string(b"payload");
        let n = blob.len();
        blob[n - 1] ^= 0xff; // flip a CRC byte
        assert!(matches!(
            deserialize_string(&blob),
            Err(RestoreParseError::BadPayload)
        ));
    }

    #[test]
    fn restore_rejects_a_too_new_version() {
        let mut blob = serialize_string(b"payload");
        let footer = blob.len() - 10;
        blob[footer..footer + 2].copy_from_slice(&(SUPPORTED_RDB_VERSION + 1).to_le_bytes());
        // Recompute the CRC so ONLY the version triggers the rejection.
        let crc = crc64(0, &blob[..blob.len() - 8]);
        let bl = blob.len();
        blob[bl - 8..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_string(&blob),
            Err(RestoreParseError::BadPayload)
        ));
    }

    /// Rewrite a DUMP blob's footer version to `ver` and recompute the CRC so ONLY the version
    /// distinguishes acceptance from rejection (the CRC always matches).
    fn with_footer_version(mut blob: Vec<u8>, ver: u16) -> Vec<u8> {
        let footer = blob.len() - 10;
        blob[footer..footer + 2].copy_from_slice(&ver.to_le_bytes());
        let crc = crc64(0, &blob[..blob.len() - 8]);
        let bl = blob.len();
        blob[bl - 8..].copy_from_slice(&crc.to_le_bytes());
        blob
    }

    #[test]
    fn restore_accepts_a_modern_redis_string_footer_version() {
        // A plain-string DUMP produced by Redis 7.4 / 8.x stamps footer version 12/13/14. The string
        // type-0 encoding is version-independent, so RESTORE must accept it (was rejected when the
        // cap was 11, silently breaking migration of a string value FROM a modern redis).
        for ver in [11u16, 12, 13, 14] {
            let blob = with_footer_version(serialize_string(b"hello-from-modern-redis"), ver);
            assert_eq!(
                deserialize_string(&blob).unwrap(),
                b"hello-from-modern-redis",
                "a version-{ver} string DUMP must RESTORE"
            );
        }
        // One past the cap is still refused (we never blindly trust an unbounded future version).
        let too_new = with_footer_version(serialize_string(b"hello"), SUPPORTED_RDB_VERSION + 1);
        assert!(matches!(
            deserialize_string(&too_new),
            Err(RestoreParseError::BadPayload)
        ));
    }

    #[test]
    fn restore_rejects_a_nonstring_type_even_at_a_modern_version() {
        // Raising the accepted version must NOT let a non-string type through: a type byte we do not
        // decode is refused as BadData regardless of the (now-accepted) footer version.
        let mut payload = vec![16u8]; // RDB_TYPE_HASH-ish: any type != RDB_TYPE_STRING
        payload.extend_from_slice(b"\x00"); // a byte of body
        payload.extend_from_slice(&14u16.to_le_bytes()); // modern, now-accepted version
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_string(&payload),
            Err(RestoreParseError::BadData)
        ));
    }

    #[test]
    fn dump_then_restore_recreates_the_value_via_the_store() {
        let mut s = test_store();
        seed(&mut s, b"src", b"the-value");
        let dumped = cmd_dump(&mut s, 0, NOW, &req(&[b"DUMP", b"src"]));
        let blob = match dumped {
            Value::BulkString(Some(b)) => b,
            other => panic!("DUMP should be a bulk string, got {other:?}"),
        };
        // RESTORE into a fresh key with no ttl.
        let r = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"dst", b"0", &blob]));
        assert_eq!(r, Value::ok());
        let got = s.read(0, b"dst", NOW).map(|v| v.as_bytes().to_vec());
        assert_eq!(got.as_deref(), Some(&b"the-value"[..]));
    }

    #[test]
    fn dump_missing_key_is_null() {
        let mut s = test_store();
        assert_eq!(
            cmd_dump(&mut s, 0, NOW, &req(&[b"DUMP", b"nope"])),
            Value::Null
        );
    }

    #[test]
    fn restore_onto_existing_key_is_busykey_unless_replace() {
        let mut s = test_store();
        seed(&mut s, b"k", b"v");
        let blob = match cmd_dump(&mut s, 0, NOW, &req(&[b"DUMP", b"k"])) {
            Value::BulkString(Some(b)) => b,
            other => panic!("{other:?}"),
        };
        // No REPLACE -> BUSYKEY, value untouched.
        let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob]));
        assert_eq!(
            match err {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-BUSYKEY Target key name already exists."
        );
        // REPLACE -> OK.
        assert_eq!(
            cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"k", b"0", &blob, b"REPLACE"])
            ),
            Value::ok()
        );
    }

    #[test]
    fn restore_sets_the_ttl() {
        let mut s = test_store();
        seed(&mut s, b"src", b"v");
        let blob = match cmd_dump(&mut s, 0, NOW, &req(&[b"DUMP", b"src"])) {
            Value::BulkString(Some(b)) => b,
            o => panic!("{o:?}"),
        };
        // Relative TTL of 50s -> absolute now + 50000.
        cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"d", b"50000", &blob]));
        assert_eq!(
            s.read(0, b"d", NOW).and_then(|v| v.expire_at()),
            Some(UnixMillis(NOW.0 + 50_000))
        );
    }

    #[test]
    fn restore_rejects_a_negative_ttl() {
        let mut s = test_store();
        let blob = serialize_string(b"v");
        let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"-1", &blob]));
        assert_eq!(
            match err {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-ERR Invalid TTL value, must be >= 0"
        );
    }

    // ---- Review-driven hardening tests. ----

    #[test]
    fn lzf_with_a_huge_declared_ulen_is_rejected_without_allocating() {
        // A HOSTILE blob: LZF encoding, a 1-byte compressed stream, but a declared ulen of ~1 TiB
        // (via the 0x81 64-bit RDB length). The pre-review code fed that straight to
        // Vec::with_capacity and aborted the process; now it must be a clean BadData with no giant
        // allocation.
        let mut payload = vec![RDB_TYPE_STRING, 0xC3]; // RDB_ENCVAL | LZF
        write_rdb_len(&mut payload, 1); // clen = 1
        payload.push(0x81); // 64-bit ulen marker
        payload.extend_from_slice(&(1u64 << 40).to_be_bytes()); // ulen = 2^40
        payload.push(0x00); // the 1 compressed byte
        payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(
            deserialize_string(&payload),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn a_raw_length_that_overflows_usize_is_bad_data_not_a_panic() {
        // A raw string declaring a 64-bit length near usize::MAX must reject cleanly (the checked_add
        // guards the range computation from an overflow panic under overflow-checks builds).
        let mut payload = vec![RDB_TYPE_STRING, 0x81]; // 64-bit length marker
        payload.extend_from_slice(&u64::MAX.to_be_bytes());
        payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(
            deserialize_string(&payload),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn parse_i64_matches_redis_string2ll_strictness() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"12345"), Some(12345));
        assert_eq!(parse_i64(b"-7"), Some(-7));
        // Rejections redis's string2ll also makes:
        assert_eq!(parse_i64(b"+5"), None, "leading + rejected");
        assert_eq!(parse_i64(b"007"), None, "leading zeros rejected");
        assert_eq!(parse_i64(b"-0"), None, "negative zero rejected");
        assert_eq!(parse_i64(b" 5"), None, "whitespace rejected");
        assert_eq!(parse_i64(b""), None, "empty rejected");
        assert_eq!(parse_i64(b"9x"), None, "trailing junk rejected");
    }

    #[test]
    fn restore_busykey_precedes_a_bad_payload_check() {
        // redis error PRECEDENCE: an existing key without REPLACE is BUSYKEY even when the payload is
        // ALSO corrupt (the existence check comes first). A garbage blob onto an existing key must
        // therefore say BUSYKEY, not "bad payload", and must not mutate the key.
        let mut s = test_store();
        seed(&mut s, b"k", b"original");
        let garbage = b"\x00\x00not a real dump blob at all!!";
        let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", garbage]));
        assert_eq!(
            match err {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-BUSYKEY Target key name already exists."
        );
        assert_eq!(
            s.read(0, b"k", NOW)
                .map(|v| v.as_bytes().to_vec())
                .as_deref(),
            Some(&b"original"[..]),
            "the existing value must be untouched"
        );
    }

    #[test]
    fn restore_freq_and_idletime_are_range_validated() {
        let mut s = test_store();
        let blob = serialize_string(b"v");
        let line = |v: Value| match v {
            Value::Error(e) => e.line(),
            o => panic!("{o:?}"),
        };
        // FREQ out of [0,255].
        assert_eq!(
            line(cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"a", b"0", &blob, b"FREQ", b"999"])
            )),
            "-ERR Invalid FREQ value, must be >= 0 and <= 255"
        );
        // IDLETIME negative.
        assert_eq!(
            line(cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"b", b"0", &blob, b"IDLETIME", b"-1"])
            )),
            "-ERR Invalid IDLETIME value, must be >= 0"
        );
        // A non-integer FREQ argument is NOT-an-integer, not a syntax error.
        assert_eq!(
            line(cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"c", b"0", &blob, b"FREQ", b"x"])
            )),
            "-ERR value is not an integer or out of range"
        );
        // A valid FREQ is accepted + ignored (value still restored).
        assert_eq!(
            cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"d", b"0", &blob, b"FREQ", b"5"])
            ),
            Value::ok()
        );
    }

    // ---- SET RESTORE (#612 phase 2): the three RDB set encodings. ----

    use std::collections::BTreeSet;

    /// Wrap a value payload (`type || body`) as a full DUMP blob: append the version + CRC-64 footer
    /// so `verify_footer` accepts it. Mirrors `serialize_string`'s footer so a test can hand-build a
    /// golden SET blob for any of the three encodings.
    fn set_blob(type_byte: u8, body: &[u8]) -> Vec<u8> {
        let mut payload = vec![type_byte];
        payload.extend_from_slice(body);
        payload.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
        let crc = crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        payload
    }

    /// Append an RDB raw string (length-prefix + bytes) to `out`. This is how redis wraps an intset /
    /// listpack blob (and each plain-set member) inside a DUMP payload.
    fn push_rdb_string(out: &mut Vec<u8>, s: &[u8]) {
        write_rdb_len(out, s.len() as u64);
        out.extend_from_slice(s);
    }

    /// Build an intset blob (`encoding[u32 LE] length[u32 LE]` then the LE integers) at the given
    /// width (2/4/8). The header `length` is `values.len()`; ordering is the caller's to control (a
    /// valid intset is strictly ascending, but a reject test wants a descending one).
    fn build_intset(encoding: u32, values: &[i64]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&encoding.to_le_bytes());
        out.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            match encoding {
                2 => out.extend_from_slice(&(v as i16).to_le_bytes()),
                4 => out.extend_from_slice(&(v as i32).to_le_bytes()),
                8 => out.extend_from_slice(&v.to_le_bytes()),
                _ => unreachable!("test intset encoding is 2/4/8"),
            }
        }
        out
    }

    /// Encode a listpack 6-bit string entry (`10xxxxxx` len + bytes), len 0..=63.
    fn lp_str6(s: &[u8]) -> Vec<u8> {
        assert!(s.len() <= 63);
        let mut o = vec![0x80 | s.len() as u8];
        o.extend_from_slice(s);
        o
    }

    /// Encode a listpack int16 entry (`0xF1` + 2 LE payload bytes).
    fn lp_int16(v: i16) -> Vec<u8> {
        let mut o = vec![0xF1];
        o.extend_from_slice(&v.to_le_bytes());
        o
    }

    /// Assemble a listpack from pre-encoded `encoding + payload` entries: the 6-byte header, each
    /// entry followed by its 1-byte reverse-encoded backlen (every test entry is < 128 bytes so the
    /// backlen is a single byte equal to the entry length), then the 0xFF EOF, with `total_bytes`
    /// fixed to the real length. Mirrors the builder proven in `rdb`'s own tests.
    fn build_listpack(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        for e in entries {
            assert!(
                e.len() <= 127,
                "test listpack entries use the 1-byte backlen"
            );
            body.extend_from_slice(e);
            body.push(e.len() as u8);
        }
        let total = 6 + body.len() + 1; // header + entries + EOF
        let mut lp = Vec::with_capacity(total);
        lp.extend_from_slice(&(total as u32).to_le_bytes());
        lp.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        lp.extend_from_slice(&body);
        lp.push(0xFF);
        lp
    }

    /// The SMEMBERS snapshot of `key` as a set of member byte-vectors (empty if absent / not a set),
    /// read through the typed `SetValue` view for order-independent assertions.
    fn set_members(store: &mut TestStore, key: &[u8]) -> BTreeSet<Vec<u8>> {
        store.rmw_mut(0, key, NOW, |entry| {
            let members = match entry {
                RmwEntry::OccupiedMut(mut o) => {
                    o.as_set_mut().map(|s| s.members()).unwrap_or_default()
                }
                _ => Vec::new(),
            };
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: members.into_iter().collect(),
            }
        })
    }

    /// SISMEMBER through the typed `SetValue` view.
    fn set_contains(store: &mut TestStore, key: &[u8], member: &[u8]) -> bool {
        store.rmw_mut(0, key, NOW, |entry| {
            let hit = match entry {
                RmwEntry::OccupiedMut(mut o) => o.as_set_mut().is_some_and(|s| s.contains(member)),
                _ => false,
            };
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: hit,
            }
        })
    }

    #[test]
    fn restore_intset_set_yields_decimal_string_members() {
        // A real-redis intset-encoded set: RESTORE must materialize each integer as its DECIMAL ASCII
        // text (redis `ll2string`), so SMEMBERS yields "-5","1","2","300".
        let mut s = test_store();
        let mut body = Vec::new();
        push_rdb_string(&mut body, &build_intset(2, &[-5, 1, 2, 300]));
        let blob = set_blob(RDB_TYPE_SET_INTSET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"iset", b"0", &blob])),
            Value::ok()
        );
        let want: BTreeSet<Vec<u8>> = [&b"-5"[..], b"1", b"2", b"300"]
            .into_iter()
            .map(<[u8]>::to_vec)
            .collect();
        assert_eq!(set_members(&mut s, b"iset"), want);
        assert_eq!(set_members(&mut s, b"iset").len(), 4); // SCARD
        assert!(set_contains(&mut s, b"iset", b"300")); // SISMEMBER hit
        assert!(!set_contains(&mut s, b"iset", b"301")); // SISMEMBER miss
    }

    #[test]
    fn restore_listpack_set_yields_string_and_int_members() {
        // A listpack-encoded set with mixed string + int elements: the ints render as decimal ASCII,
        // the strings are the raw bytes.
        let mut s = test_store();
        let lp = build_listpack(&[
            lp_str6(b"hello"),
            lp_int16(-5),
            lp_str6(b"world"),
            lp_int16(42),
        ]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_SET_LISTPACK, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"lset", b"0", &blob])),
            Value::ok()
        );
        let want: BTreeSet<Vec<u8>> = [&b"hello"[..], b"-5", b"world", b"42"]
            .into_iter()
            .map(<[u8]>::to_vec)
            .collect();
        assert_eq!(set_members(&mut s, b"lset"), want);
        assert!(set_contains(&mut s, b"lset", b"hello"));
        assert!(set_contains(&mut s, b"lset", b"-5"));
    }

    #[test]
    fn restore_plain_set_yields_string_members() {
        // A plain RDB_TYPE_SET (redis's hashtable encoding on DUMP): a member count then that many
        // RDB strings.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 3);
        push_rdb_string(&mut body, b"alpha");
        push_rdb_string(&mut body, b"beta");
        push_rdb_string(&mut body, b"gamma");
        let blob = set_blob(RDB_TYPE_SET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"pset", b"0", &blob])),
            Value::ok()
        );
        let want: BTreeSet<Vec<u8>> = [&b"alpha"[..], b"beta", b"gamma"]
            .into_iter()
            .map(<[u8]>::to_vec)
            .collect();
        assert_eq!(set_members(&mut s, b"pset"), want);
    }

    #[test]
    fn restore_plain_set_dedups_repeated_members() {
        // NewValueOwned::Set dedups (via SetVal::from_members), so a hand-built blob with a repeat
        // still yields the unique members. Real redis never dumps a duplicate, but the decoder must
        // not trust the input to be unique.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 3);
        push_rdb_string(&mut body, b"dup");
        push_rdb_string(&mut body, b"dup");
        push_rdb_string(&mut body, b"unique");
        let blob = set_blob(RDB_TYPE_SET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"d", b"0", &blob])),
            Value::ok()
        );
        let got = set_members(&mut s, b"d");
        assert_eq!(got.len(), 2, "the duplicate must collapse");
        assert!(got.contains(&b"dup"[..]) && got.contains(&b"unique"[..]));
    }

    #[test]
    fn restore_set_honors_replace_and_ttl() {
        let mut s = test_store();
        // Restore a plain set with no ttl.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 2);
        push_rdb_string(&mut body, b"x");
        push_rdb_string(&mut body, b"y");
        let blob = set_blob(RDB_TYPE_SET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])),
            Value::ok()
        );
        // Restoring onto the existing key without REPLACE is BUSYKEY (value untouched).
        assert_eq!(
            match cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])) {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-BUSYKEY Target key name already exists."
        );
        // REPLACE + a relative ttl: an intset overwrites the set and the deadline is now + ttl.
        let mut ibody = Vec::new();
        push_rdb_string(&mut ibody, &build_intset(2, &[7, 8, 9]));
        let iblob = set_blob(RDB_TYPE_SET_INTSET, &ibody);
        assert_eq!(
            cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"k", b"25000", &iblob, b"REPLACE"])
            ),
            Value::ok()
        );
        let want: BTreeSet<Vec<u8>> = [&b"7"[..], b"8", b"9"]
            .into_iter()
            .map(<[u8]>::to_vec)
            .collect();
        assert_eq!(set_members(&mut s, b"k"), want);
        assert_eq!(
            s.read(0, b"k", NOW).and_then(|v| v.expire_at()),
            Some(UnixMillis(NOW.0 + 25_000))
        );
    }

    #[test]
    fn deserialize_set_rejects_a_huge_declared_count_without_allocating() {
        // A plain RDB_TYPE_SET whose declared member count (~4 billion) dwarfs the tiny body: the
        // count-vs-remaining bound rejects BEFORE the pre-allocation, no over-alloc, no panic.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 0xffff_ffff);
        push_rdb_string(&mut body, b"only-one");
        let blob = set_blob(RDB_TYPE_SET, &body);
        assert_eq!(deserialize_set(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn deserialize_set_rejects_a_listpack_length_past_the_end() {
        // A listpack whose header total_bytes lies far past the real slice must be BadData with no
        // over-read (the listpack decoder's exact-length gate).
        let mut lp = build_listpack(&[lp_str6(b"a")]);
        lp[0..4].copy_from_slice(&0xffff_ffffu32.to_le_bytes());
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_SET_LISTPACK, &body);
        assert_eq!(deserialize_set(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn deserialize_set_rejects_a_non_ascending_intset() {
        // A descending intset is not a valid (sorted, unique) intset.
        let mut body = Vec::new();
        push_rdb_string(&mut body, &build_intset(2, &[5, 3, 1]));
        let blob = set_blob(RDB_TYPE_SET_INTSET, &body);
        assert_eq!(deserialize_set(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn restore_a_hostile_set_blob_errors_and_creates_no_key() {
        // End-to-end: a hostile SET blob returns the bad-data error and leaves NO key behind (no
        // panic, no partial write).
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 0xffff_ffff);
        push_rdb_string(&mut body, b"x");
        let blob = set_blob(RDB_TYPE_SET, &body);
        let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"h", b"0", &blob]));
        assert_eq!(
            match err {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-ERR Bad data format"
        );
        assert!(s.read(0, b"h", NOW).is_none(), "no key must be created");
    }

    // ---- HASH RESTORE (#612 phase 3): the two non-field-TTL RDB hash encodings. ----

    use std::collections::BTreeMap;

    /// The HGETALL snapshot of `key` as a `field -> value` map (empty if absent / not a hash), read
    /// through the typed `HashValue` view for order-independent assertions (HLEN is `.len()`, HGET is a
    /// lookup).
    fn hash_pairs(store: &mut TestStore, key: &[u8]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        store.rmw_mut(0, key, NOW, |entry| {
            let pairs = match entry {
                RmwEntry::OccupiedMut(mut o) => {
                    o.as_hash_mut().map(|h| h.pairs()).unwrap_or_default()
                }
                _ => Vec::new(),
            };
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: pairs.into_iter().collect(),
            }
        })
    }

    #[test]
    fn restore_plain_hash_yields_string_pairs() {
        // A plain RDB_TYPE_HASH (redis's hashtable encoding on DUMP): a PAIR count then that many
        // field/value RDB strings, read alternately.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 3); // three field/value PAIRS
        push_rdb_string(&mut body, b"f1");
        push_rdb_string(&mut body, b"v1");
        push_rdb_string(&mut body, b"f2");
        push_rdb_string(&mut body, b"v2");
        push_rdb_string(&mut body, b"f3");
        push_rdb_string(&mut body, b"v3");
        let blob = set_blob(RDB_TYPE_HASH, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"phash", b"0", &blob])),
            Value::ok()
        );
        let got = hash_pairs(&mut s, b"phash");
        assert_eq!(got.len(), 3); // HLEN
        assert_eq!(got.get(&b"f1"[..]).map(Vec::as_slice), Some(&b"v1"[..])); // HGET
        assert_eq!(got.get(&b"f2"[..]).map(Vec::as_slice), Some(&b"v2"[..]));
        assert_eq!(got.get(&b"f3"[..]).map(Vec::as_slice), Some(&b"v3"[..]));
    }

    #[test]
    fn restore_listpack_hash_yields_string_and_int_pairs() {
        // A listpack-encoded hash with mixed string + int fields AND values: the ints render as decimal
        // ASCII, the strings are raw bytes. Elements are the flattened pairs field,value,field,value.
        let mut s = test_store();
        let lp = build_listpack(&[
            lp_str6(b"name"),
            lp_str6(b"bob"),
            lp_str6(b"age"),
            lp_int16(42), // an INT value -> "42"
            lp_int16(7),  // an INT field -> "7"
            lp_str6(b"lucky"),
        ]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_HASH_LISTPACK, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"lhash", b"0", &blob])),
            Value::ok()
        );
        let got = hash_pairs(&mut s, b"lhash");
        assert_eq!(got.len(), 3);
        assert_eq!(got.get(&b"name"[..]).map(Vec::as_slice), Some(&b"bob"[..]));
        assert_eq!(got.get(&b"age"[..]).map(Vec::as_slice), Some(&b"42"[..]));
        assert_eq!(got.get(&b"7"[..]).map(Vec::as_slice), Some(&b"lucky"[..]));
    }

    #[test]
    fn restore_listpack_hash_repeated_field_keeps_last_value() {
        // A hand-built listpack hash with the SAME field twice: NewValueOwned::Hash builds via
        // HashVal::set (last write overwrites in place), so the LAST value wins, matching Redis. Real
        // redis never dumps a duplicate field, but the decoder must not trust the input to be unique.
        let mut s = test_store();
        let lp = build_listpack(&[
            lp_str6(b"k"),
            lp_str6(b"first"),
            lp_str6(b"k"),
            lp_str6(b"second"),
        ]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_HASH_LISTPACK, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"dup", b"0", &blob])),
            Value::ok()
        );
        let got = hash_pairs(&mut s, b"dup");
        assert_eq!(got.len(), 1, "the repeated field must collapse to one");
        assert_eq!(
            got.get(&b"k"[..]).map(Vec::as_slice),
            Some(&b"second"[..]),
            "last value wins"
        );
    }

    #[test]
    fn deserialize_hash_rejects_an_odd_listpack() {
        // A hash listpack must hold an EVEN number of elements (field/value pairs); an odd count is a
        // corrupt/hostile blob and is BadData (no partial pair kept).
        let lp = build_listpack(&[lp_str6(b"a"), lp_str6(b"b"), lp_str6(b"c")]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_HASH_LISTPACK, &body);
        assert_eq!(deserialize_hash(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn restore_hash_field_ttl_type_is_unsupported_and_creates_no_key() {
        // The field-TTL hash encodings (HASH_LISTPACK_EX=25, HASH_METADATA=24, and the pre-GA 23/22)
        // are DEFERRED to PR4: each must be a clean bad-data error with NO key created (never a
        // half-decoded / TTL-dropped hash).
        for &ty in &[
            RDB_TYPE_HASH_LISTPACK_EX,
            RDB_TYPE_HASH_METADATA,
            RDB_TYPE_HASH_LISTPACK_EX_PRE_GA,
            RDB_TYPE_HASH_METADATA_PRE_GA,
        ] {
            let mut s = test_store();
            let blob = set_blob(ty, &[0x00]); // any body; the type byte is refused before decode
            let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"ex", b"0", &blob]));
            assert_eq!(
                match err {
                    Value::Error(e) => e.line(),
                    o => panic!("type {ty}: {o:?}"),
                },
                "-ERR Bad data format",
                "field-TTL type {ty} must be a clean bad-data error"
            );
            assert!(
                s.read(0, b"ex", NOW).is_none(),
                "type {ty}: no key must be created"
            );
        }
    }

    #[test]
    fn deserialize_hash_rejects_a_huge_declared_count_without_allocating() {
        // A plain RDB_TYPE_HASH whose declared pair count (~4 billion) dwarfs the tiny body: the
        // 2*count-vs-remaining bound rejects BEFORE the pre-allocation, no over-alloc, no panic.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 0xffff_ffff);
        push_rdb_string(&mut body, b"only-field");
        push_rdb_string(&mut body, b"only-value");
        let blob = set_blob(RDB_TYPE_HASH, &body);
        assert_eq!(deserialize_hash(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn deserialize_hash_rejects_a_listpack_length_past_the_end() {
        // A hash listpack whose header total_bytes lies far past the real slice must be BadData with no
        // over-read (the listpack decoder's exact-length gate).
        let mut lp = build_listpack(&[lp_str6(b"f"), lp_str6(b"v")]);
        lp[0..4].copy_from_slice(&0xffff_ffffu32.to_le_bytes());
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_HASH_LISTPACK, &body);
        assert_eq!(deserialize_hash(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn restore_hash_honors_replace_and_ttl() {
        let mut s = test_store();
        // Restore a plain hash with no ttl.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 2);
        push_rdb_string(&mut body, b"a");
        push_rdb_string(&mut body, b"1");
        push_rdb_string(&mut body, b"b");
        push_rdb_string(&mut body, b"2");
        let blob = set_blob(RDB_TYPE_HASH, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])),
            Value::ok()
        );
        // Restoring onto the existing key without REPLACE is BUSYKEY (value untouched).
        assert_eq!(
            match cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])) {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-BUSYKEY Target key name already exists."
        );
        // REPLACE + a relative ttl: a listpack hash overwrites the value and the deadline is now + ttl.
        let lp = build_listpack(&[lp_str6(b"x"), lp_int16(9)]);
        let mut lbody = Vec::new();
        push_rdb_string(&mut lbody, &lp);
        let lblob = set_blob(RDB_TYPE_HASH_LISTPACK, &lbody);
        assert_eq!(
            cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"k", b"30000", &lblob, b"REPLACE"])
            ),
            Value::ok()
        );
        let got = hash_pairs(&mut s, b"k");
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(&b"x"[..]).map(Vec::as_slice), Some(&b"9"[..]));
        assert_eq!(
            s.read(0, b"k", NOW).and_then(|v| v.expire_at()),
            Some(UnixMillis(NOW.0 + 30_000))
        );
    }

    // ---- ZSET RESTORE (#612 phase 5): the three RDB sorted-set encodings. ----

    /// Append a legacy ASCII score (redis `rdbSaveDoubleValue` for a finite value): a 1-byte length
    /// then the ASCII float text (the +inf/-inf/NaN sentinels are pushed as a bare 254/255/253 byte).
    fn push_ascii_score(out: &mut Vec<u8>, text: &[u8]) {
        assert!(
            text.len() < 253,
            "ascii score text uses a plain length byte"
        );
        out.push(text.len() as u8);
        out.extend_from_slice(text);
    }

    /// Append an 8-byte little-endian binary-double score (redis `rdbSaveBinaryDoubleValue`).
    fn push_binary_score(out: &mut Vec<u8>, score: f64) {
        out.extend_from_slice(&score.to_le_bytes());
    }

    /// The ordered `(member, score)` pairs of `key` (ZRANGE 0 -1 WITHSCORES), read through the typed
    /// `ZSetValue` view for a deterministic (score, member)-ordered assertion; empty if absent / not a
    /// zset. `.len()` on the result is ZCARD.
    fn zset_ordered(store: &mut TestStore, key: &[u8]) -> Vec<(Vec<u8>, f64)> {
        store.rmw_mut(0, key, NOW, |entry| {
            let pairs = match entry {
                RmwEntry::OccupiedMut(mut o) => o
                    .as_zset_mut()
                    .map(|z| z.range_by_rank(0, -1, false))
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: pairs,
            }
        })
    }

    /// ZSCORE through the typed `ZSetValue` view.
    fn zset_score(store: &mut TestStore, key: &[u8], member: &[u8]) -> Option<f64> {
        store.rmw_mut(0, key, NOW, |entry| {
            let sc = match entry {
                RmwEntry::OccupiedMut(mut o) => o.as_zset_mut().and_then(|z| z.score(member)),
                _ => None,
            };
            RmwStep {
                action: RmwAction::Keep,
                expire: ExpireWrite::Unchanged,
                reply: sc,
            }
        })
    }

    #[test]
    fn restore_zset2_binary_double_scores() {
        // RDB_TYPE_ZSET_2: an RDB length = member count, then (member, 8-byte LE binary double). Covers
        // a NEGATIVE score, a FRACTIONAL score, and +inf; ZRANGE WITHSCORES comes back in
        // (score, member) order and each ZSCORE matches.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 3);
        push_rdb_string(&mut body, b"neg");
        push_binary_score(&mut body, -3.5);
        push_rdb_string(&mut body, b"frac");
        push_binary_score(&mut body, 1.5);
        push_rdb_string(&mut body, b"top");
        push_binary_score(&mut body, f64::INFINITY);
        let blob = set_blob(RDB_TYPE_ZSET_2, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"z2", b"0", &blob])),
            Value::ok()
        );
        assert_eq!(
            zset_ordered(&mut s, b"z2"),
            vec![
                (b"neg".to_vec(), -3.5),
                (b"frac".to_vec(), 1.5),
                (b"top".to_vec(), f64::INFINITY),
            ]
        );
        assert_eq!(zset_score(&mut s, b"z2", b"frac"), Some(1.5)); // ZSCORE
        assert_eq!(zset_ordered(&mut s, b"z2").len(), 3); // ZCARD
    }

    #[test]
    fn restore_legacy_zset_ascii_scores_with_neg_inf_sentinel() {
        // RDB_TYPE_ZSET (legacy): a member count then (member, ASCII score). The -inf sentinel (a bare
        // length byte 255) and a normal "2" score both decode; -inf sorts first.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 2);
        push_rdb_string(&mut body, b"bottom");
        body.push(255); // -inf sentinel (rdbSaveDoubleValue)
        push_rdb_string(&mut body, b"mid");
        push_ascii_score(&mut body, b"2");
        let blob = set_blob(RDB_TYPE_ZSET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"zl", b"0", &blob])),
            Value::ok()
        );
        assert_eq!(
            zset_ordered(&mut s, b"zl"),
            vec![
                (b"bottom".to_vec(), f64::NEG_INFINITY),
                (b"mid".to_vec(), 2.0),
            ]
        );
        assert_eq!(
            zset_score(&mut s, b"zl", b"bottom"),
            Some(f64::NEG_INFINITY)
        );
    }

    #[test]
    fn restore_zset_listpack_int_and_fractional_scores() {
        // RDB_TYPE_ZSET_LISTPACK: flattened [member, score, ...]. A member-as-int renders decimal
        // ("7"); an int score is that integer as f64 (5.0); a string score is parsed ("2.5").
        let mut s = test_store();
        let lp = build_listpack(&[
            lp_str6(b"a"),
            lp_int16(5),     // int score -> 5.0
            lp_int16(7),     // int MEMBER -> "7"
            lp_str6(b"2.5"), // string score -> 2.5
        ]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        let blob = set_blob(RDB_TYPE_ZSET_LISTPACK, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"zlp", b"0", &blob])),
            Value::ok()
        );
        assert_eq!(
            zset_ordered(&mut s, b"zlp"),
            vec![(b"7".to_vec(), 2.5), (b"a".to_vec(), 5.0)]
        );
        assert_eq!(zset_score(&mut s, b"zlp", b"7"), Some(2.5));
        assert_eq!(zset_score(&mut s, b"zlp", b"a"), Some(5.0));
    }

    #[test]
    fn restore_zset_repeated_member_keeps_last_score() {
        // A hand-built ZSET_2 with the SAME member twice: NewValueOwned::ZSet builds via
        // ZSetVal::from_pairs (last score wins), matching Redis. Real redis never dumps a duplicate,
        // but the decoder must not trust the input to be unique.
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 3);
        push_rdb_string(&mut body, b"a");
        push_binary_score(&mut body, 1.0);
        push_rdb_string(&mut body, b"b");
        push_binary_score(&mut body, 2.0);
        push_rdb_string(&mut body, b"a");
        push_binary_score(&mut body, 9.0);
        let blob = set_blob(RDB_TYPE_ZSET_2, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"dup", b"0", &blob])),
            Value::ok()
        );
        assert_eq!(zset_ordered(&mut s, b"dup").len(), 2, "duplicate collapses");
        assert_eq!(
            zset_score(&mut s, b"dup", b"a"),
            Some(9.0),
            "last score wins"
        );
    }

    #[test]
    fn deserialize_zset_rejects_nan_in_every_encoding() {
        // A NaN score is corrupt in ALL three encodings (parity with our ZADD guard + Redis's post-load
        // isnan check): the binary bit pattern, the legacy 253 sentinel, and a listpack "nan" text.
        let mut b2 = Vec::new();
        write_rdb_len(&mut b2, 1);
        push_rdb_string(&mut b2, b"m");
        push_binary_score(&mut b2, f64::NAN);
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_2, &b2)),
            Err(RestoreParseError::BadData)
        );

        let mut bl = Vec::new();
        write_rdb_len(&mut bl, 1);
        push_rdb_string(&mut bl, b"m");
        bl.push(253); // NaN sentinel
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET, &bl)),
            Err(RestoreParseError::BadData)
        );

        let lp = build_listpack(&[lp_str6(b"m"), lp_str6(b"nan")]);
        let mut blp = Vec::new();
        push_rdb_string(&mut blp, &lp);
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_LISTPACK, &blp)),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn deserialize_zset_rejects_an_odd_listpack() {
        // A zset listpack must hold an EVEN number of elements (member/score pairs); an odd count is a
        // corrupt/hostile blob (no partial pair kept).
        let lp = build_listpack(&[lp_str6(b"a"), lp_int16(1), lp_str6(b"b")]);
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_LISTPACK, &body)),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn deserialize_zset_rejects_a_huge_declared_count_without_allocating() {
        // A ZSET_2 whose declared count (~4 billion) dwarfs the tiny body: the count*9-vs-remaining
        // bound rejects BEFORE the pre-allocation, no over-alloc, no panic.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 0xffff_ffff);
        push_rdb_string(&mut body, b"m");
        push_binary_score(&mut body, 1.0);
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_2, &body)),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn deserialize_zset_rejects_a_truncated_binary_double() {
        // A ZSET_2 element with a member long enough to pass the count bound, but only 3 of the 8 score
        // bytes: the binary-double reader rejects the short tail as BadData, no over-read.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 1);
        push_rdb_string(&mut body, b"member"); // 7 bytes encoded, so count*9 <= remaining
        body.extend_from_slice(&[0u8; 3]); // only 3 of 8 score bytes
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_2, &body)),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn deserialize_zset_rejects_a_listpack_length_past_the_end() {
        // A zset listpack whose header total_bytes lies far past the real slice must be BadData with no
        // over-read (the listpack decoder's exact-length gate).
        let mut lp = build_listpack(&[lp_str6(b"a"), lp_int16(1)]);
        lp[0..4].copy_from_slice(&0xffff_ffffu32.to_le_bytes());
        let mut body = Vec::new();
        push_rdb_string(&mut body, &lp);
        assert_eq!(
            deserialize_zset(&set_blob(RDB_TYPE_ZSET_LISTPACK, &body)),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn restore_a_hostile_zset_blob_errors_and_creates_no_key() {
        // End-to-end: a hostile ZSET blob returns the bad-data error and leaves NO key behind (no
        // panic, no partial write).
        let mut s = test_store();
        let mut body = Vec::new();
        write_rdb_len(&mut body, 0xffff_ffff);
        push_rdb_string(&mut body, b"x");
        push_binary_score(&mut body, 1.0);
        let blob = set_blob(RDB_TYPE_ZSET_2, &body);
        let err = cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"h", b"0", &blob]));
        assert_eq!(
            match err {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-ERR Bad data format"
        );
        assert!(s.read(0, b"h", NOW).is_none(), "no key must be created");
    }

    #[test]
    fn restore_zset_honors_replace_and_ttl() {
        let mut s = test_store();
        // Restore a legacy ASCII zset with no ttl.
        let mut body = Vec::new();
        write_rdb_len(&mut body, 2);
        push_rdb_string(&mut body, b"a");
        push_ascii_score(&mut body, b"1");
        push_rdb_string(&mut body, b"b");
        push_ascii_score(&mut body, b"2");
        let blob = set_blob(RDB_TYPE_ZSET, &body);
        assert_eq!(
            cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])),
            Value::ok()
        );
        // Without REPLACE -> BUSYKEY (value untouched).
        assert_eq!(
            match cmd_restore(&mut s, 0, NOW, &req(&[b"RESTORE", b"k", b"0", &blob])) {
                Value::Error(e) => e.line(),
                o => panic!("{o:?}"),
            },
            "-BUSYKEY Target key name already exists."
        );
        // REPLACE + a relative ttl: a ZSET_2 blob overwrites the value and the deadline is now + ttl.
        let mut body2 = Vec::new();
        write_rdb_len(&mut body2, 1);
        push_rdb_string(&mut body2, b"x");
        push_binary_score(&mut body2, 3.5);
        let blob2 = set_blob(RDB_TYPE_ZSET_2, &body2);
        assert_eq!(
            cmd_restore(
                &mut s,
                0,
                NOW,
                &req(&[b"RESTORE", b"k", b"40000", &blob2, b"REPLACE"])
            ),
            Value::ok()
        );
        assert_eq!(zset_ordered(&mut s, b"k"), vec![(b"x".to_vec(), 3.5)]);
        assert_eq!(
            s.read(0, b"k", NOW).and_then(|v| v.expire_at()),
            Some(UnixMillis(NOW.0 + 40_000))
        );
    }
}
