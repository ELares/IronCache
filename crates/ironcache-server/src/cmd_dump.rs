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
//! The STRING type (`RDB_TYPE_STRING = 0`). Because a HyperLogLog is stored AS a string
//! (`cmd_hll`), this gives HLL DUMP/RESTORE byte-interop for free (#242 part 2: an HLL DUMPed here
//! RESTOREs + PFCOUNTs identically on redis). Other value types (list/set/hash/zset) are a tracked
//! follow-up; DUMP of one is a typed "unsupported" error rather than a wrong blob.
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

/// The RDB opcode for a plain string value (`RDB_TYPE_STRING`).
const RDB_TYPE_STRING: u8 = 0;

/// The RDB version stamped into a DUMP footer. Redis's `verifyDumpPayload` accepts any version `<=`
/// the server's `RDB_VERSION`, so a conservative 9 (Redis 5/6) is RESTORE-able by every redis >= 6.0
/// while the type-0 string encoding is version-independent.
const DUMP_RDB_VERSION: u16 = 9;

/// The highest RDB footer version RESTORE accepts (matching a modern redis, 7.2-era). A newer version
/// is refused as [`ErrorReply::restore_bad_payload`], exactly as redis refuses a too-new payload.
const SUPPORTED_RDB_VERSION: u16 = 11;

/// The largest value RESTORE will reconstruct: the Redis 512 MiB bulk-string cap
/// ([bulk-string-max-512mb], KEYSPACE.md). A declared length above this is a hostile/garbage payload,
/// rejected as [`RestoreParseError::BadData`] BEFORE any allocation, so an attacker cannot make a tiny
/// blob demand a huge buffer (the LZF `ulen` DoS the review flagged).
const MAX_RESTORE_VALUE_BYTES: usize = 512 * 1024 * 1024;

/// The ceiling on how much a decoder PRE-allocates from a still-unverified declared length: a legit
/// value still grows past this via `push`/`extend`, but a tiny blob that lies about its size can never
/// force more than this up front. The final exact-length check is the correctness gate.
const DECODE_PREALLOC_CAP: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// CRC-64 (Jones variant, the Redis DUMP checksum): width 64, poly 0xad93d23594c935a9, reflected in
// and out, init 0. Validated against the published check value CRC64("123456789") = 0xe9c6d914c4b8d9ca.
// ---------------------------------------------------------------------------

/// The reflected form of the Jones polynomial (computed once at compile time; the bit-reversal is
/// checked implicitly by the CRC known-answer test).
const CRC64_POLY_REFLECTED: u64 = reflect64(0xad93_d235_94c9_35a9);

/// Bit-reverse a 64-bit value (for building the reflected polynomial).
const fn reflect64(mut x: u64) -> u64 {
    let mut r = 0u64;
    let mut i = 0;
    while i < 64 {
        r = (r << 1) | (x & 1);
        x >>= 1;
        i += 1;
    }
    r
}

/// CRC-64/Jones over `data`, continuing from `crc` (start at 0). The bitwise reflected algorithm; a
/// byte-at-a-time loop, plenty fast for a bounded DUMP payload and free of any table to get wrong.
fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    for &b in data {
        crc ^= u64::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC64_POLY_REFLECTED
            } else {
                crc >> 1
            };
        }
    }
    crc
}

// ---------------------------------------------------------------------------
// RDB length + string encoding.
// ---------------------------------------------------------------------------

/// Append the RDB length encoding of `len` to `out` (RDB_6BITLEN / RDB_14BITLEN / RDB_32BITLEN /
/// RDB_64BITLEN). `RDB_ENCVAL` (the `11xxxxxx` special encodings) is only produced on DECODE.
fn write_rdb_len(out: &mut Vec<u8>, len: u64) {
    if len < 64 {
        out.push(len as u8); // 00xxxxxx
    } else if len < 16384 {
        out.push(0x40 | (len >> 8) as u8); // 01xxxxxx yyyyyyyy
        out.push((len & 0xff) as u8);
    } else if let Ok(len32) = u32::try_from(len) {
        out.push(0x80); // RDB_32BITLEN, then 4 bytes big-endian
        out.extend_from_slice(&len32.to_be_bytes());
    } else {
        out.push(0x81); // RDB_64BITLEN, then 8 bytes big-endian
        out.extend_from_slice(&len.to_be_bytes());
    }
}

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

/// A DUMP-payload parse failure, mapped to the byte-exact redis RESTORE error.
#[derive(Debug, PartialEq, Eq)]
enum RestoreParseError {
    /// The footer (version/CRC) is missing, too-new, or the checksum mismatches.
    BadPayload,
    /// The payload parsed structurally but its body is not a decodable value (unknown type,
    /// truncated length, bad LZF, or trailing garbage).
    BadData,
}

impl RestoreParseError {
    fn into_reply(self) -> ErrorReply {
        match self {
            RestoreParseError::BadPayload => ErrorReply::restore_bad_payload(),
            RestoreParseError::BadData => ErrorReply::restore_bad_data(),
        }
    }
}

/// Verify a DUMP blob's footer (version <= supported, CRC-64 matches) and return the value-payload
/// slice (everything before the 10-byte footer).
fn verify_footer(blob: &[u8]) -> Result<&[u8], RestoreParseError> {
    if blob.len() < 11 {
        // The smallest valid string blob is type(1) + len(1) + empty + version(2) + crc(8) = 12; a
        // blob shorter than the footer + one type byte cannot be valid.
        return Err(RestoreParseError::BadPayload);
    }
    let footer_at = blob.len() - 10;
    let version = u16::from_le_bytes([blob[footer_at], blob[footer_at + 1]]);
    if version > SUPPORTED_RDB_VERSION {
        return Err(RestoreParseError::BadPayload);
    }
    let stored_crc = u64::from_le_bytes(blob[footer_at + 2..].try_into().expect("8 bytes"));
    // The CRC covers the value bytes AND the 2-byte version (everything except the 8-byte CRC).
    let computed = crc64(0, &blob[..blob.len() - 8]);
    if computed != stored_crc {
        return Err(RestoreParseError::BadPayload);
    }
    Ok(&blob[..footer_at])
}

/// Read an RDB length (or a special `RDB_ENCVAL` marker) from `p` at `*pos`, advancing `*pos`. On a
/// special encoding, returns `Err`-carried marker via `Ok((len, is_encoded))` where `is_encoded`
/// means the "length" is actually the encoding type.
fn read_rdb_len(p: &[u8], pos: &mut usize) -> Result<(u64, bool), RestoreParseError> {
    let first = *p.get(*pos).ok_or(RestoreParseError::BadData)?;
    *pos += 1;
    match first >> 6 {
        0 => Ok((u64::from(first & 0x3f), false)), // 6-bit
        1 => {
            let lo = *p.get(*pos).ok_or(RestoreParseError::BadData)?;
            *pos += 1;
            Ok(((u64::from(first & 0x3f) << 8) | u64::from(lo), false)) // 14-bit
        }
        2 => {
            // 0x80 = 32-bit big-endian; 0x81 = 64-bit big-endian.
            match first {
                0x80 => {
                    let b = p.get(*pos..*pos + 4).ok_or(RestoreParseError::BadData)?;
                    *pos += 4;
                    Ok((u64::from(u32::from_be_bytes(b.try_into().unwrap())), false))
                }
                0x81 => {
                    let b = p.get(*pos..*pos + 8).ok_or(RestoreParseError::BadData)?;
                    *pos += 8;
                    Ok((u64::from_be_bytes(b.try_into().unwrap()), false))
                }
                _ => Err(RestoreParseError::BadData),
            }
        }
        _ => Ok((u64::from(first & 0x3f), true)), // 11xxxxxx: RDB_ENCVAL, the low 6 bits are the enc type
    }
}

/// Decode an RDB string (raw, the INT8/16/32 special encodings, or LZF compression) at `*pos`.
fn read_rdb_string(p: &[u8], pos: &mut usize) -> Result<Vec<u8>, RestoreParseError> {
    let (len_or_enc, is_encoded) = read_rdb_len(p, pos)?;
    if is_encoded {
        return match len_or_enc {
            0 => read_int_string(p, pos, 1), // RDB_ENC_INT8
            1 => read_int_string(p, pos, 2), // RDB_ENC_INT16
            2 => read_int_string(p, pos, 4), // RDB_ENC_INT32
            3 => read_lzf_string(p, pos),    // RDB_ENC_LZF
            _ => Err(RestoreParseError::BadData),
        };
    }
    let len = usize::try_from(len_or_enc).map_err(|_| RestoreParseError::BadData)?;
    // checked_add so a hostile length near usize::MAX is a clean BadData, never an overflow panic
    // (which would fire under overflow-checks builds, i.e. debug + CI).
    let end = pos.checked_add(len).ok_or(RestoreParseError::BadData)?;
    let bytes = p.get(*pos..end).ok_or(RestoreParseError::BadData)?;
    *pos = end;
    Ok(bytes.to_vec())
}

/// Decode a little-endian signed integer of `width` bytes, rendered as its DECIMAL ASCII string
/// (redis stores small integers this way; the RESTOREd value is the number's text, e.g. `12345`).
fn read_int_string(p: &[u8], pos: &mut usize, width: usize) -> Result<Vec<u8>, RestoreParseError> {
    let b = p
        .get(*pos..*pos + width)
        .ok_or(RestoreParseError::BadData)?;
    *pos += width;
    let v: i64 = match width {
        1 => i64::from(b[0] as i8),
        2 => i64::from(i16::from_le_bytes([b[0], b[1]])),
        4 => i64::from(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
        _ => return Err(RestoreParseError::BadData),
    };
    Ok(v.to_string().into_bytes())
}

/// Decompress an LZF-compressed RDB string: `clen` (compressed len) `ulen` (uncompressed len) then
/// the LZF stream. LZF is a byte stream of (a) literal runs `0LLLLLLL` + L+1 literal bytes, and (b)
/// back-references `LLLNNNNN [NNNNNNNN]` copying `len` bytes from `distance` back. Only DECOMPRESSION
/// is needed (DUMP never compresses); the format is small and validated by a known-answer test.
fn read_lzf_string(p: &[u8], pos: &mut usize) -> Result<Vec<u8>, RestoreParseError> {
    let (clen, _) = read_rdb_len(p, pos)?;
    let (ulen, _) = read_rdb_len(p, pos)?;
    let clen = usize::try_from(clen).map_err(|_| RestoreParseError::BadData)?;
    let ulen = usize::try_from(ulen).map_err(|_| RestoreParseError::BadData)?;
    // Reject an absurd declared output size BEFORE decoding: `ulen` is fully attacker-controlled (a
    // 64-bit RDB length), so bound it to the value cap so a tiny blob cannot demand a huge buffer.
    if ulen > MAX_RESTORE_VALUE_BYTES {
        return Err(RestoreParseError::BadData);
    }
    let end = pos.checked_add(clen).ok_or(RestoreParseError::BadData)?;
    let input = p.get(*pos..end).ok_or(RestoreParseError::BadData)?;
    *pos = end;
    let out = lzf_decompress(input, ulen)?;
    Ok(out)
}

/// The LZF decompressor (Marc Lehmann's liblzf, the variant Redis vendors). `expected` is the known
/// output length; a stream that over/under-runs it or references before the start is `BadData`.
fn lzf_decompress(input: &[u8], expected: usize) -> Result<Vec<u8>, RestoreParseError> {
    // Pre-allocate only up to a cap: a blob that lies (`expected` huge, stream tiny) grows the vec to
    // its ACTUAL output and then fails the exact-length gate below, so it can never force a giant
    // up-front allocation. A legit large value still grows naturally as bytes are pushed.
    let mut out: Vec<u8> = Vec::with_capacity(expected.min(DECODE_PREALLOC_CAP));
    let mut i = 0usize;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            // A literal run of ctrl+1 bytes.
            let run = ctrl + 1;
            let lit = input.get(i..i + run).ok_or(RestoreParseError::BadData)?;
            out.extend_from_slice(lit);
            i += run;
        } else {
            // A back-reference. High 3 bits of ctrl are (len-1) unless == 7, then an extra length byte.
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(i).ok_or(RestoreParseError::BadData)? as usize;
                i += 1;
            }
            // Distance: low 5 bits of ctrl (high) + the next byte (low), then +1.
            let lo = *input.get(i).ok_or(RestoreParseError::BadData)? as usize;
            i += 1;
            let distance = ((ctrl & 0x1f) << 8) | lo;
            let mut ref_pos = out
                .len()
                .checked_sub(distance + 1)
                .ok_or(RestoreParseError::BadData)?;
            // Copy len+2 bytes, one at a time (ranges can overlap the output being written).
            for _ in 0..len + 2 {
                let byte = *out.get(ref_pos).ok_or(RestoreParseError::BadData)?;
                out.push(byte);
                ref_pos += 1;
            }
        }
    }
    if out.len() != expected {
        return Err(RestoreParseError::BadData);
    }
    Ok(out)
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
        match deserialize_string(blob) {
            // Vacant, or Occupied with REPLACE: write the value (Replace on a vacant entry inserts).
            Ok(value) => RmwStep {
                action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(value))),
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
    fn crc64_matches_the_published_jones_check_value() {
        // The canonical CRC-64/Jones (redis) check: crc64(0, "123456789") = 0xe9c6d914c4b8d9ca.
        assert_eq!(crc64(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
        assert_eq!(crc64(0, b""), 0);
    }

    #[test]
    fn rdb_len_encoding_covers_the_size_classes() {
        let mut out = Vec::new();
        write_rdb_len(&mut out, 5);
        assert_eq!(out, [5]); // 6-bit
        out.clear();
        write_rdb_len(&mut out, 300);
        assert_eq!(out, [0x40 | 1, 44]); // 14-bit: 300 = 0x12C -> (1<<8)|44
        out.clear();
        write_rdb_len(&mut out, 100_000);
        assert_eq!(out[0], 0x80); // 32-bit marker
        assert_eq!(&out[1..], &100_000u32.to_be_bytes());
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
}
