// SPDX-License-Identifier: MIT OR Apache-2.0
//! The shared RDB codec substrate for `DUMP` / `RESTORE` (#612).
//!
//! This module is the reusable, store-free half of the Redis/Valkey DUMP format: the CRC-64 footer,
//! the RDB length + string encodings, and the container element iterators (listpack + intset). It
//! carries NO command wiring and NO knowledge of the value store; `cmd_dump` (the STRING path today,
//! the aggregate types in the follow-up PRs) builds on these primitives.
//!
//! The primitives were extracted VERBATIM from `cmd_dump.rs` (the string DUMP path, #129) so every
//! DUMP/RESTORE behavior is unchanged; the two container decoders are new. Both halves share ONE
//! defensive discipline: every declared length is `checked_add` / `checked_mul` validated against the
//! remaining bytes BEFORE any slice or allocation, pre-allocation is capped at [`DECODE_PREALLOC_CAP`],
//! and an unknown sub-encoding, a truncated entry, a count mismatch, or trailing garbage is a clean
//! [`RestoreParseError::BadData`] rather than a panic or an over-read on a hostile blob.

// PR1 (#612) is the substrate ONLY: the aggregate RDB type bytes and the container iterators
// (`listpack_iter` / `intset_iter`) are exercised by the tests below but are not yet wired into a
// command path, so they read as dead code until the per-type PRs call them. Allow it module-wide
// rather than sprinkle the substrate with per-item attributes; the extracted STRING primitives stay
// proven-used by `cmd_dump` and its tests.
#![allow(dead_code)]

use ironcache_protocol::ErrorReply;

// ---------------------------------------------------------------------------
// RDB value type bytes (Redis `src/rdb.h`). Only [`RDB_TYPE_STRING`] is wired today; the aggregate
// type bytes are declared here so the per-type PRs (#612) dispatch on a named constant, not a magic
// number. Values are the on-disk RDB opcodes, confirmed against `redis/src/rdb.h`.
// ---------------------------------------------------------------------------

/// The RDB opcode for a plain string value (`RDB_TYPE_STRING`).
pub(crate) const RDB_TYPE_STRING: u8 = 0;
/// A plain (non-encoded) list of RDB strings (`RDB_TYPE_LIST`, legacy).
pub(crate) const RDB_TYPE_LIST: u8 = 1;
/// A plain (non-encoded) set of RDB strings (`RDB_TYPE_SET`).
pub(crate) const RDB_TYPE_SET: u8 = 2;
/// A sorted set with ASCII-encoded scores (`RDB_TYPE_ZSET`, legacy version 1).
pub(crate) const RDB_TYPE_ZSET: u8 = 3;
/// A plain hash of RDB string field/value pairs (`RDB_TYPE_HASH`).
pub(crate) const RDB_TYPE_HASH: u8 = 4;
/// A sorted set with binary-`double` scores (`RDB_TYPE_ZSET_2`).
pub(crate) const RDB_TYPE_ZSET_2: u8 = 5;
/// A hash stored as a zipmap (`RDB_TYPE_HASH_ZIPMAP`, very old, decode-only compat).
pub(crate) const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
/// A list stored as a single ziplist (`RDB_TYPE_LIST_ZIPLIST`, legacy).
pub(crate) const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
/// A set stored as an intset (`RDB_TYPE_SET_INTSET`); the body is decoded by [`intset_iter`].
pub(crate) const RDB_TYPE_SET_INTSET: u8 = 11;
/// A sorted set stored as a ziplist (`RDB_TYPE_ZSET_ZIPLIST`, legacy).
pub(crate) const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
/// A hash stored as a ziplist (`RDB_TYPE_HASH_ZIPLIST`, legacy).
pub(crate) const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
/// A list of ziplists, the first quicklist format (`RDB_TYPE_LIST_QUICKLIST`, legacy).
pub(crate) const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
/// A hash stored as a listpack (`RDB_TYPE_HASH_LISTPACK`); the body is decoded by [`listpack_iter`].
pub(crate) const RDB_TYPE_HASH_LISTPACK: u8 = 16;
/// A sorted set stored as a listpack (`RDB_TYPE_ZSET_LISTPACK`); decoded by [`listpack_iter`].
pub(crate) const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
/// A list of listpacks, the current quicklist format (`RDB_TYPE_LIST_QUICKLIST_2`).
pub(crate) const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
/// A set stored as a listpack (`RDB_TYPE_SET_LISTPACK`); the body is decoded by [`listpack_iter`].
pub(crate) const RDB_TYPE_SET_LISTPACK: u8 = 20;
/// A hash with field TTLs, metadata form, 7.4 release candidate (`RDB_TYPE_HASH_METADATA_PRE_GA`).
pub(crate) const RDB_TYPE_HASH_METADATA_PRE_GA: u8 = 22;
/// A hash-with-field-TTLs listpack, 7.4 release candidate (`RDB_TYPE_HASH_LISTPACK_EX_PRE_GA`).
pub(crate) const RDB_TYPE_HASH_LISTPACK_EX_PRE_GA: u8 = 23;
/// A hash with field TTLs, GA metadata form (`RDB_TYPE_HASH_METADATA`).
pub(crate) const RDB_TYPE_HASH_METADATA: u8 = 24;
/// A hash-with-field-TTLs listpack, GA form (`RDB_TYPE_HASH_LISTPACK_EX`).
pub(crate) const RDB_TYPE_HASH_LISTPACK_EX: u8 = 25;

// ---------------------------------------------------------------------------
// Quicklist node container tags (Redis `src/quicklist.h`). Inside a `RDB_TYPE_LIST_QUICKLIST_2`
// body, each node is prefixed by an RDB length whose value is one of these two container tags: a
// PLAIN node holds a single raw element, a PACKED node holds a listpack of elements. They are RDB
// lengths (read with [`read_rdb_len`]), so they are typed `u64` to compare against that reader's
// return directly.
// ---------------------------------------------------------------------------

/// A quicklist-2 node holding a SINGLE raw element, stored unpacked
/// (`QUICKLIST_NODE_CONTAINER_PLAIN`). The node body is that one element's bytes, read as an RDB
/// string.
pub(crate) const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
/// A quicklist-2 node holding a LISTPACK of elements (`QUICKLIST_NODE_CONTAINER_PACKED`). The node
/// body is a listpack (itself stored AS an RDB string) decoded by [`listpack_iter`].
pub(crate) const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

// ---------------------------------------------------------------------------
// Footer / bound constants.
// ---------------------------------------------------------------------------

/// The RDB version stamped into a DUMP footer. Redis's `verifyDumpPayload` accepts any version `<=`
/// the server's `RDB_VERSION`, so a conservative 9 (Redis 5/6) is RESTORE-able by every redis >= 6.0
/// while the type-0 string encoding is version-independent.
pub(crate) const DUMP_RDB_VERSION: u16 = 9;

/// The highest RDB footer version RESTORE accepts (matching Redis 8.x `RDB_VERSION`). A newer version
/// is refused as [`ErrorReply::restore_bad_payload`], exactly as redis refuses a too-new payload.
///
/// Accepting up through 14 (rather than the older 11) lets a plain-string DUMP produced by a modern
/// Redis/Valkey (which stamp a higher footer as new AGGREGATE encodings ship) RESTORE cleanly here.
/// This is SOUND because our decoder only reconstructs the type-0 STRING encoding, which is
/// version-independent (raw / INT8-16-32 / LZF, unchanged for many releases), and rejects any unknown
/// TYPE byte or string encoding as [`RestoreParseError::BadData`] REGARDLESS of the footer version.
/// So a v14 non-string blob is still refused on its type byte, not silently mis-decoded; only the
/// version-stable string case is newly accepted. Our own DUMP still stamps the conservative
/// `DUMP_RDB_VERSION` for maximum backward RESTORE-ability elsewhere (dump-low, accept-high).
pub(crate) const SUPPORTED_RDB_VERSION: u16 = 14;

/// The largest value RESTORE will reconstruct: the Redis 512 MiB bulk-string cap
/// ([bulk-string-max-512mb], KEYSPACE.md). A declared length above this is a hostile/garbage payload,
/// rejected as [`RestoreParseError::BadData`] BEFORE any allocation, so an attacker cannot make a tiny
/// blob demand a huge buffer (the LZF `ulen` DoS the review flagged).
pub(crate) const MAX_RESTORE_VALUE_BYTES: usize = 512 * 1024 * 1024;

/// The ceiling on how much a decoder PRE-allocates from a still-unverified declared length: a legit
/// value still grows past this via `push`/`extend`, but a tiny blob that lies about its size can never
/// force more than this up front. The final exact-length check is the correctness gate.
pub(crate) const DECODE_PREALLOC_CAP: usize = 64 * 1024;

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
pub(crate) fn crc64(mut crc: u64, data: &[u8]) -> u64 {
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
// The RESTORE parse error.
// ---------------------------------------------------------------------------

/// A DUMP-payload parse failure, mapped to the byte-exact redis RESTORE error.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RestoreParseError {
    /// The footer (version/CRC) is missing, too-new, or the checksum mismatches.
    BadPayload,
    /// The payload parsed structurally but its body is not a decodable value (unknown type,
    /// truncated length, bad LZF, or trailing garbage).
    BadData,
}

impl RestoreParseError {
    pub(crate) fn into_reply(self) -> ErrorReply {
        match self {
            RestoreParseError::BadPayload => ErrorReply::restore_bad_payload(),
            RestoreParseError::BadData => ErrorReply::restore_bad_data(),
        }
    }
}

// ---------------------------------------------------------------------------
// RDB footer + length + string encoding.
// ---------------------------------------------------------------------------

/// Append the RDB length encoding of `len` to `out` (RDB_6BITLEN / RDB_14BITLEN / RDB_32BITLEN /
/// RDB_64BITLEN). `RDB_ENCVAL` (the `11xxxxxx` special encodings) is only produced on DECODE.
pub(crate) fn write_rdb_len(out: &mut Vec<u8>, len: u64) {
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

/// Verify a DUMP blob's footer (version <= supported, CRC-64 matches) and return the value-payload
/// slice (everything before the 10-byte footer).
pub(crate) fn verify_footer(blob: &[u8]) -> Result<&[u8], RestoreParseError> {
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
pub(crate) fn read_rdb_len(p: &[u8], pos: &mut usize) -> Result<(u64, bool), RestoreParseError> {
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
pub(crate) fn read_rdb_string(p: &[u8], pos: &mut usize) -> Result<Vec<u8>, RestoreParseError> {
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
pub(crate) fn read_int_string(
    p: &[u8],
    pos: &mut usize,
    width: usize,
) -> Result<Vec<u8>, RestoreParseError> {
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

// ---------------------------------------------------------------------------
// RDB double (zset score) encodings. A sorted set stores each score either as a raw 8-byte
// little-endian IEEE754 binary64 (`RDB_TYPE_ZSET_2`, Redis `rdbLoadBinaryDoubleValue`) or, in the
// legacy `RDB_TYPE_ZSET`, as a length-prefixed ASCII string with three sentinel lengths for the
// non-finite values (`rdbLoadDoubleValue`). Both readers return the RAW `f64` -- a +inf/-inf is a
// legitimate score and is preserved -- and leave a NaN for the caller to reject, matching Redis's
// post-load `isnan` guard (`rdbReportCorruptRDB("Zset with NAN score detected")`) and our own ZADD.
// ---------------------------------------------------------------------------

/// Read an 8-byte little-endian IEEE754 `binary64` score (`RDB_TYPE_ZSET_2`, Redis
/// `rdbLoadBinaryDoubleValue`, which stores the double little-endian byte-for-byte -- `memrev64ifbe`
/// is a no-op on the little-endian wire form). Returns the raw value: a +inf/-inf is legitimate and a
/// NaN bit pattern is returned as-is for the caller's `is_nan` gate to reject. A truncated 8-byte
/// tail is a clean [`RestoreParseError::BadData`], never an over-read.
pub(crate) fn read_rdb_binary_double(p: &[u8], pos: &mut usize) -> Result<f64, RestoreParseError> {
    let b = p.get(*pos..*pos + 8).ok_or(RestoreParseError::BadData)?;
    *pos += 8;
    Ok(f64::from_le_bytes(b.try_into().expect("8 bytes")))
}

/// Read a legacy ASCII score (`RDB_TYPE_ZSET`, Redis `rdbLoadDoubleValue`): one length byte `L`, then
/// the three sentinel lengths `255` -> -inf, `254` -> +inf, `253` -> NaN (returned as NaN for the
/// caller to reject), else read `L` bytes and parse them as an ASCII float. Returns the raw value; a
/// truncated body or a non-float text is a clean [`RestoreParseError::BadData`].
pub(crate) fn read_rdb_ascii_double(p: &[u8], pos: &mut usize) -> Result<f64, RestoreParseError> {
    let len = *p.get(*pos).ok_or(RestoreParseError::BadData)?;
    *pos += 1;
    match len {
        255 => Ok(f64::NEG_INFINITY),
        254 => Ok(f64::INFINITY),
        // The NaN sentinel: returned as NaN so the caller's single `is_nan` gate turns it into
        // BadData, exactly as Redis's post-load isnan check rejects a NaN-scored zset element.
        253 => Ok(f64::NAN),
        n => {
            let n = usize::from(n);
            let end = pos.checked_add(n).ok_or(RestoreParseError::BadData)?;
            let bytes = p.get(*pos..end).ok_or(RestoreParseError::BadData)?;
            *pos = end;
            parse_ascii_double(bytes)
        }
    }
}

/// Parse a raw ASCII float (the digits Redis writes with `ll2string` / `fpconv_dtoa`, or an `inf` /
/// `-inf` spelling some producers emit) into an `f64`. Redis loads these with `sscanf(%lg)`; Rust's
/// float parser accepts the same finite decimal / scientific forms plus the `inf`/`infinity`
/// spellings. A byte string that is not a valid float is [`RestoreParseError::BadData`]; a `nan` text
/// parses to NaN and is left for the caller's `is_nan` gate to reject. Also used for a listpack
/// zset's string-encoded score element.
pub(crate) fn parse_ascii_double(bytes: &[u8]) -> Result<f64, RestoreParseError> {
    let s = std::str::from_utf8(bytes).map_err(|_| RestoreParseError::BadData)?;
    s.parse::<f64>().map_err(|_| RestoreParseError::BadData)
}

/// Decompress an LZF-compressed RDB string: `clen` (compressed len) `ulen` (uncompressed len) then
/// the LZF stream. LZF is a byte stream of (a) literal runs `0LLLLLLL` + L+1 literal bytes, and (b)
/// back-references `LLLNNNNN [NNNNNNNN]` copying `len` bytes from `distance` back. Only DECOMPRESSION
/// is needed (DUMP never compresses); the format is small and validated by a known-answer test.
pub(crate) fn read_lzf_string(p: &[u8], pos: &mut usize) -> Result<Vec<u8>, RestoreParseError> {
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
pub(crate) fn lzf_decompress(input: &[u8], expected: usize) -> Result<Vec<u8>, RestoreParseError> {
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

// ---------------------------------------------------------------------------
// Listpack (Redis `src/listpack.c`). A listpack is `total_bytes[u32 LE] num_elements[u16 LE]` then a
// run of self-describing entries, each `encoding-byte + payload + backlen`, terminated by a 0xFF EOF
// byte. The backlen is a reverse-encoded (1..=5 byte) copy of the entry's `encoding + payload` length,
// used for backward iteration; a FORWARD decoder that already computed the entry length just skips
// `lpEncodeBacklenBytes(entry_len)` bytes and never reads the trailer. Confirmed against the encoding
// macros and `lpEncodeBacklen` / `lpCurrentEncodedSizeUnsafe` in `redis/src/listpack.c`.
// ---------------------------------------------------------------------------

/// The listpack header: `total_bytes[u32 LE]` + `num_elements[u16 LE]`.
const LP_HDR_SIZE: usize = 6;
/// The listpack end-of-listpack marker byte.
const LP_EOF: u8 = 0xFF;

/// A decoded listpack element: an integer (any of the fixed int encodings) or a raw byte string. The
/// aggregate decoders in the later PRs interpret these as members / field-value pairs / score pairs.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LpElem {
    Int(i64),
    Str(Box<[u8]>),
}

/// Decode a Redis listpack blob into its elements, in order. Rejects a header whose declared
/// `total_bytes` does not match the slice, an unknown encoding byte, a truncated entry, an element
/// count that disagrees with the header, or trailing bytes after the EOF marker; every length is
/// bounds-checked before any slice, so a hostile blob is a clean [`RestoreParseError::BadData`].
pub(crate) fn listpack_iter(bytes: &[u8]) -> Result<Vec<LpElem>, RestoreParseError> {
    if bytes.len() < LP_HDR_SIZE + 1 {
        // Need the 6-byte header plus at least the 1-byte EOF marker.
        return Err(RestoreParseError::BadData);
    }
    let total_bytes = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let num_elements = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
    // The header's self-declared length must match the slice EXACTLY and stay under the value cap: a
    // header that lies about its size can never drive a slice past the end or a large pre-alloc.
    if total_bytes != bytes.len() || total_bytes > MAX_RESTORE_VALUE_BYTES {
        return Err(RestoreParseError::BadData);
    }
    let mut out = Vec::with_capacity(num_elements.min(DECODE_PREALLOC_CAP));
    let mut pos = LP_HDR_SIZE;
    loop {
        let first = *bytes.get(pos).ok_or(RestoreParseError::BadData)?;
        if first == LP_EOF {
            pos += 1;
            break;
        }
        // Decode encoding + payload; `entry_len` is the encoding+payload byte count (the backlen
        // trailer is NOT included). `lp_decode_entry` bounds-checks every payload read.
        let (elem, entry_len) = lp_decode_entry(bytes, pos)?;
        out.push(elem);
        // Advance past the entry, then past the backlen trailer whose width is a pure function of the
        // entry length (Redis lpEncodeBacklenBytes). Both hops are checked_add-gated and range-checked
        // so a truncated tail is BadData, never an over-read.
        pos = pos
            .checked_add(entry_len)
            .ok_or(RestoreParseError::BadData)?;
        pos = pos
            .checked_add(lp_backlen_bytes(entry_len))
            .ok_or(RestoreParseError::BadData)?;
        if pos > bytes.len() {
            return Err(RestoreParseError::BadData);
        }
    }
    // After the EOF byte there must be no trailing garbage, and the decoded count must match the
    // header (num_elements is the true count for the small aggregate encodings we decode; the Redis
    // "unknown" sentinel does not arise there).
    if pos != bytes.len() || out.len() != num_elements {
        return Err(RestoreParseError::BadData);
    }
    Ok(out)
}

/// Decode ONE listpack entry (encoding byte + payload) at `pos`, returning the element and the number
/// of bytes the encoding + payload occupy (the backlen trailer is NOT counted). Every declared string
/// length is `checked_add`-gated against `bytes` before the slice, so a hostile length is a clean
/// [`RestoreParseError::BadData`], never a panic or over-read. The caller handles the 0xFF EOF byte.
fn lp_decode_entry(bytes: &[u8], pos: usize) -> Result<(LpElem, usize), RestoreParseError> {
    let first = *bytes.get(pos).ok_or(RestoreParseError::BadData)?;
    // 0xxxxxxx: 7-bit unsigned int, 0..=127, encoded in the byte itself.
    if first & 0x80 == 0 {
        return Ok((LpElem::Int(i64::from(first & 0x7f)), 1));
    }
    // 10xxxxxx: 6-bit string, low 6 bits are the length, then that many data bytes.
    if first & 0xC0 == 0x80 {
        let len = usize::from(first & 0x3f);
        let s = lp_take_str(bytes, pos + 1, len)?;
        return Ok((LpElem::Str(s), 1 + len));
    }
    // 110xxxxx yyyyyyyy: 13-bit signed int, high 5 bits + next byte, two's complement.
    if first & 0xE0 == 0xC0 {
        let b1 = *bytes.get(pos + 1).ok_or(RestoreParseError::BadData)?;
        let raw = i64::from((u32::from(first & 0x1f) << 8) | u32::from(b1));
        // Sign-extend from bit 12: shift the 13 significant bits to the top of the i64 and back down
        // arithmetically (64 - 13 = 51).
        return Ok((LpElem::Int((raw << 51) >> 51), 2));
    }
    // 1110xxxx yyyyyyyy: 12-bit string, high 4 bits + next byte are the length, then the data bytes.
    if first & 0xF0 == 0xE0 {
        let b1 = *bytes.get(pos + 1).ok_or(RestoreParseError::BadData)?;
        let len = (usize::from(first & 0x0f) << 8) | usize::from(b1);
        let s = lp_take_str(bytes, pos + 2, len)?;
        return Ok((LpElem::Str(s), 2 + len));
    }
    // The remaining forms are the fixed 0xF0..=0xF4 encodings (EOF 0xFF is handled by the caller).
    match first {
        0xF0 => {
            // 32-bit string: the next 4 bytes (little-endian) are the length, then the data bytes.
            let lb = bytes
                .get(pos + 1..pos + 5)
                .ok_or(RestoreParseError::BadData)?;
            let len = u32::from_le_bytes([lb[0], lb[1], lb[2], lb[3]]) as usize;
            let s = lp_take_str(bytes, pos + 5, len)?;
            Ok((LpElem::Str(s), 5 + len))
        }
        0xF1 => {
            // int16, 2 payload bytes little-endian.
            let b = bytes
                .get(pos + 1..pos + 3)
                .ok_or(RestoreParseError::BadData)?;
            Ok((LpElem::Int(i64::from(i16::from_le_bytes([b[0], b[1]]))), 3))
        }
        0xF2 => {
            // int24, 3 payload bytes little-endian, sign-extended from bit 23 (64 - 24 = 40).
            let b = bytes
                .get(pos + 1..pos + 4)
                .ok_or(RestoreParseError::BadData)?;
            let raw = i64::from(u32::from(b[0]) | (u32::from(b[1]) << 8) | (u32::from(b[2]) << 16));
            Ok((LpElem::Int((raw << 40) >> 40), 4))
        }
        0xF3 => {
            // int32, 4 payload bytes little-endian.
            let b = bytes
                .get(pos + 1..pos + 5)
                .ok_or(RestoreParseError::BadData)?;
            Ok((
                LpElem::Int(i64::from(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))),
                5,
            ))
        }
        0xF4 => {
            // int64, 8 payload bytes little-endian.
            let b = bytes
                .get(pos + 1..pos + 9)
                .ok_or(RestoreParseError::BadData)?;
            Ok((LpElem::Int(i64::from_le_bytes(b.try_into().unwrap())), 9))
        }
        // 0xF5..=0xFE are unassigned encodings; anything here is unknown -> reject.
        _ => Err(RestoreParseError::BadData),
    }
}

/// Take a `len`-byte string payload starting at `start`, bounds-checked against `bytes`. The length
/// is capped and `checked_add`-gated so a hostile 32-bit length near `usize::MAX` is a clean
/// [`RestoreParseError::BadData`], never an over-read or an overflow panic.
fn lp_take_str(bytes: &[u8], start: usize, len: usize) -> Result<Box<[u8]>, RestoreParseError> {
    if len > MAX_RESTORE_VALUE_BYTES {
        return Err(RestoreParseError::BadData);
    }
    let end = start.checked_add(len).ok_or(RestoreParseError::BadData)?;
    let s = bytes.get(start..end).ok_or(RestoreParseError::BadData)?;
    Ok(s.to_vec().into_boxed_slice())
}

/// The width in bytes of the reverse-encoded backlen trailer for an entry whose encoding + payload
/// occupies `entry_len` bytes (Redis `lpEncodeBacklenBytes`: 7 significant bits per byte). A forward
/// decoder that already knows `entry_len` skips exactly this many bytes without reading the trailer.
fn lp_backlen_bytes(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else if entry_len <= 16383 {
        2
    } else if entry_len <= 2_097_151 {
        3
    } else if entry_len <= 268_435_455 {
        4
    } else {
        5
    }
}

// ---------------------------------------------------------------------------
// Intset (Redis `src/intset.c`). An intset is `encoding[u32 LE] length[u32 LE]` then `length` signed
// little-endian integers of `encoding` (2, 4, or 8) bytes each, stored strictly ascending and unique.
// Confirmed against `intset` struct + `_intsetGetEncoded` in `redis/src/intset.c`.
// ---------------------------------------------------------------------------

/// The intset header: `encoding[u32 LE]` + `length[u32 LE]`.
const INTSET_HDR_SIZE: usize = 8;

/// Decode a Redis intset blob (the `RDB_TYPE_SET_INTSET` body) into its integers. Validates the
/// declared `length * encoding` matches the slice EXACTLY and asserts the values are strictly
/// ascending (a real intset is sorted and unique); anything else is [`RestoreParseError::BadData`].
pub(crate) fn intset_iter(bytes: &[u8]) -> Result<Vec<i64>, RestoreParseError> {
    if bytes.len() < INTSET_HDR_SIZE {
        return Err(RestoreParseError::BadData);
    }
    let encoding = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    if !matches!(encoding, 2 | 4 | 8) {
        return Err(RestoreParseError::BadData);
    }
    // The declared payload must fit the value cap AND account for the slice EXACTLY: checked_mul /
    // checked_add so a hostile length*encoding is a clean BadData, never an overflow or over-read.
    let payload = length
        .checked_mul(encoding)
        .ok_or(RestoreParseError::BadData)?;
    if payload > MAX_RESTORE_VALUE_BYTES {
        return Err(RestoreParseError::BadData);
    }
    let total = INTSET_HDR_SIZE
        .checked_add(payload)
        .ok_or(RestoreParseError::BadData)?;
    if total != bytes.len() {
        return Err(RestoreParseError::BadData);
    }
    let mut out: Vec<i64> = Vec::with_capacity(length.min(DECODE_PREALLOC_CAP));
    let mut pos = INTSET_HDR_SIZE;
    let mut prev: Option<i64> = None;
    for _ in 0..length {
        // `pos + encoding` stays within `total == bytes.len()` by construction, but slice via `get`
        // so a corrupt header can never over-read.
        let b = bytes
            .get(pos..pos + encoding)
            .ok_or(RestoreParseError::BadData)?;
        let v: i64 = match encoding {
            2 => i64::from(i16::from_le_bytes([b[0], b[1]])),
            4 => i64::from(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            8 => i64::from_le_bytes(b.try_into().unwrap()),
            _ => unreachable!("encoding validated to 2, 4, or 8 above"),
        };
        // A real intset is sorted ascending and unique; a non-ascending (or duplicate) blob is corrupt
        // or hostile.
        if prev.is_some_and(|p| v <= p) {
            return Err(RestoreParseError::BadData);
        }
        prev = Some(v);
        out.push(v);
        pos += encoding;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // ---- Listpack test builder. ----
    //
    // These helpers assemble a listpack byte-for-byte per the spec `listpack_iter` implements (each
    // encoder mirrors the matching branch of Redis `lpEncodeIntegerGetType` / `lpEncodeString`, and
    // `lp_build` appends the reverse-encoded backlen and fixes `total_bytes`). Building the blob from
    // the same spec is a self-consistency (round-trip) check; TRUE Redis parity is confirmed by the
    // differential oracle once a command path wires this in a later PR (#612).

    /// Encode one listpack entry's `encoding + payload` bytes (no backlen) for a 7-bit uint.
    fn lp_u7(v: u8) -> Vec<u8> {
        assert!(v <= 127);
        vec![v]
    }
    /// Encode a 6-bit string entry (len 0..=63).
    fn lp_str6(s: &[u8]) -> Vec<u8> {
        assert!(s.len() <= 63);
        let mut o = vec![0x80 | s.len() as u8];
        o.extend_from_slice(s);
        o
    }
    /// Encode a 13-bit signed int entry (-4096..=4095).
    fn lp_i13(v: i16) -> Vec<u8> {
        assert!((-4096..=4095).contains(&v));
        let u = (v as u16) & 0x1fff; // low 13 bits, two's complement
        vec![0xC0 | (u >> 8) as u8, (u & 0xff) as u8]
    }
    /// Encode a 12-bit string entry (len 0..=4095).
    fn lp_str12(s: &[u8]) -> Vec<u8> {
        assert!(s.len() <= 4095);
        let len = s.len();
        let mut o = vec![0xE0 | (len >> 8) as u8, (len & 0xff) as u8];
        o.extend_from_slice(s);
        o
    }
    /// Encode a 32-bit string entry (length in the following 4 LE bytes).
    fn lp_str32(s: &[u8]) -> Vec<u8> {
        let mut o = vec![0xF0];
        o.extend_from_slice(&(s.len() as u32).to_le_bytes());
        o.extend_from_slice(s);
        o
    }
    /// Encode an int16 entry (0xF1 + 2 LE payload bytes).
    fn lp_i16(v: i16) -> Vec<u8> {
        let mut o = vec![0xF1];
        o.extend_from_slice(&v.to_le_bytes());
        o
    }
    /// Encode an int24 entry (0xF2 + 3 LE payload bytes, two's complement).
    fn lp_i24(v: i32) -> Vec<u8> {
        assert!((-8_388_608..=8_388_607).contains(&v));
        let u = (v as u32) & 0x00ff_ffff;
        vec![0xF2, (u & 0xff) as u8, (u >> 8) as u8, (u >> 16) as u8]
    }
    /// Encode an int32 entry (0xF3 + 4 LE payload bytes).
    fn lp_i32(v: i32) -> Vec<u8> {
        let mut o = vec![0xF3];
        o.extend_from_slice(&v.to_le_bytes());
        o
    }
    /// Encode an int64 entry (0xF4 + 8 LE payload bytes).
    fn lp_i64(v: i64) -> Vec<u8> {
        let mut o = vec![0xF4];
        o.extend_from_slice(&v.to_le_bytes());
        o
    }

    /// Append the reverse-encoded backlen for an entry of `entry_len` encoding+payload bytes, mirroring
    /// Redis `lpEncodeBacklen` (1..=5 bytes, 7 significant bits per byte, most significant first).
    fn lp_push_backlen(out: &mut Vec<u8>, entry_len: usize) {
        let l = entry_len as u64;
        if l <= 127 {
            out.push(l as u8);
        } else if l <= 16383 {
            out.push((l >> 7) as u8);
            out.push(((l & 127) | 128) as u8);
        } else if l <= 2_097_151 {
            out.push((l >> 14) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        } else {
            unreachable!("test entries never exceed the 3-byte backlen range");
        }
    }

    /// Assemble a full listpack from pre-encoded `encoding + payload` entries: 6-byte header, each
    /// entry followed by its backlen, then the 0xFF EOF byte, with `total_bytes` fixed to the real
    /// length. `num` is written into the header (usually `entries.len()`, but overridable to build the
    /// count-mismatch reject case).
    fn lp_build(entries: &[Vec<u8>], num: u16) -> Vec<u8> {
        let mut body = Vec::new();
        for e in entries {
            body.extend_from_slice(e);
            lp_push_backlen(&mut body, e.len());
        }
        let total = LP_HDR_SIZE + body.len() + 1; // header + entries + EOF
        let mut lp = Vec::with_capacity(total);
        lp.extend_from_slice(&(total as u32).to_le_bytes());
        lp.extend_from_slice(&num.to_le_bytes());
        lp.extend_from_slice(&body);
        lp.push(LP_EOF);
        lp
    }

    /// Assert a listpack decodes to exactly `expected`.
    fn assert_lp(entries: &[Vec<u8>], expected: &[LpElem]) {
        let blob = lp_build(entries, entries.len() as u16);
        let got = listpack_iter(&blob).expect("listpack should decode");
        assert_eq!(got.len(), expected.len(), "element count");
        for (g, e) in got.iter().zip(expected) {
            match (g, e) {
                (LpElem::Int(a), LpElem::Int(b)) => assert_eq!(a, b, "int element"),
                (LpElem::Str(a), LpElem::Str(b)) => assert_eq!(a, b, "str element"),
                _ => panic!("element kind mismatch"),
            }
        }
    }

    #[test]
    fn listpack_empty_decodes_to_no_elements() {
        let blob = lp_build(&[], 0);
        assert_eq!(listpack_iter(&blob).unwrap().len(), 0);
    }

    #[test]
    fn listpack_decodes_each_encoding() {
        // A 7-bit uint.
        assert_lp(&[lp_u7(127)], &[LpElem::Int(127)]);
        assert_lp(&[lp_u7(0)], &[LpElem::Int(0)]);
        // A 6-bit string.
        assert_lp(
            &[lp_str6(b"hello")],
            &[LpElem::Str(Box::from(&b"hello"[..]))],
        );
        assert_lp(&[lp_str6(b"")], &[LpElem::Str(Box::from(&b""[..]))]);
        // A 13-bit negative int (and a positive one at the boundary).
        assert_lp(&[lp_i13(-1)], &[LpElem::Int(-1)]);
        assert_lp(&[lp_i13(-4096)], &[LpElem::Int(-4096)]);
        assert_lp(&[lp_i13(4095)], &[LpElem::Int(4095)]);
        // A 12-bit string (len past the 6-bit range).
        let s12 = vec![b'z'; 100];
        assert_lp(&[lp_str12(&s12)], &[LpElem::Str(s12.clone().into())]);
        // The fixed-width ints: 16 / 24 / 32 / 64, positive and negative.
        assert_lp(&[lp_i16(-12_345)], &[LpElem::Int(-12_345)]);
        assert_lp(&[lp_i16(32_767)], &[LpElem::Int(32_767)]);
        assert_lp(&[lp_i24(-8_388_608)], &[LpElem::Int(-8_388_608)]);
        assert_lp(&[lp_i24(8_388_607)], &[LpElem::Int(8_388_607)]);
        assert_lp(&[lp_i32(-2_000_000_000)], &[LpElem::Int(-2_000_000_000)]);
        assert_lp(
            &[lp_i64(-9_000_000_000_000_000_000)],
            &[LpElem::Int(-9_000_000_000_000_000_000)],
        );
        assert_lp(&[lp_i64(i64::MAX)], &[LpElem::Int(i64::MAX)]);
        // A 32-bit string.
        assert_lp(
            &[lp_str32(b"a-32bit-string-payload")],
            &[LpElem::Str(Box::from(&b"a-32bit-string-payload"[..]))],
        );
    }

    #[test]
    fn listpack_decodes_a_mixed_multi_element_blob() {
        let s12 = vec![b'q'; 70];
        assert_lp(
            &[
                lp_u7(9),
                lp_str6(b"field"),
                lp_i13(-2048),
                lp_str12(&s12),
                lp_i32(123_456_789),
                lp_str32(b"tail"),
            ],
            &[
                LpElem::Int(9),
                LpElem::Str(Box::from(&b"field"[..])),
                LpElem::Int(-2048),
                LpElem::Str(s12.clone().into()),
                LpElem::Int(123_456_789),
                LpElem::Str(Box::from(&b"tail"[..])),
            ],
        );
    }

    #[test]
    fn listpack_rejects_a_count_mismatch() {
        // Header claims 2 elements but only 1 is encoded.
        let blob = lp_build(&[lp_u7(1)], 2);
        assert_eq!(listpack_iter(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn listpack_rejects_an_unknown_encoding_byte() {
        // 0xF5 is an unassigned listpack encoding.
        let mut blob = lp_build(&[lp_u7(1)], 1);
        // Overwrite the first entry byte (offset 6) with the unknown encoding; the count no longer
        // matters because the walk fails first.
        blob[LP_HDR_SIZE] = 0xF5;
        assert_eq!(listpack_iter(&blob), Err(RestoreParseError::BadData));
    }

    #[test]
    fn listpack_rejects_trailing_garbage_and_a_bad_header_len() {
        // A well-formed listpack with one extra byte appended: total_bytes no longer matches the slice.
        let mut blob = lp_build(&[lp_u7(1)], 1);
        blob.push(0x00);
        assert_eq!(listpack_iter(&blob), Err(RestoreParseError::BadData));
        // A header total_bytes far larger than the slice must reject without an over-read.
        let mut lying = lp_build(&[lp_u7(1)], 1);
        lying[0..4].copy_from_slice(&0xffff_ffffu32.to_le_bytes());
        assert_eq!(listpack_iter(&lying), Err(RestoreParseError::BadData));
    }

    #[test]
    fn listpack_dos_a_32bit_str_len_past_the_end_is_rejected_without_alloc() {
        // A 32-bit string entry whose declared length runs far past the slice: must be a clean
        // BadData with NO large allocation and NO over-read. We hand-build so total_bytes matches the
        // real (short) slice while the inner 32-bit length lies.
        let mut body = vec![0xF0u8]; // 32-bit string encoding
        body.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // declared len ~4 GiB
        body.extend_from_slice(b"tiny"); // only 4 real data bytes
        lp_push_backlen(&mut body, 1 + 4 + 4); // a plausible backlen (value is skipped, not read)
        let total = LP_HDR_SIZE + body.len() + 1;
        let mut blob = Vec::new();
        blob.extend_from_slice(&(total as u32).to_le_bytes());
        blob.extend_from_slice(&1u16.to_le_bytes());
        blob.extend_from_slice(&body);
        blob.push(LP_EOF);
        assert_eq!(listpack_iter(&blob), Err(RestoreParseError::BadData));
    }

    // ---- Intset test builder + goldens. ----

    /// Assemble an intset blob from `values` at the given `encoding` (2/4/8), header written to match.
    fn intset_build(encoding: u32, values: &[i64]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&encoding.to_le_bytes());
        out.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            match encoding {
                2 => out.extend_from_slice(&(v as i16).to_le_bytes()),
                4 => out.extend_from_slice(&(v as i32).to_le_bytes()),
                8 => out.extend_from_slice(&v.to_le_bytes()),
                _ => unreachable!(),
            }
        }
        out
    }

    #[test]
    fn intset_decodes_each_width() {
        // 16-bit.
        assert_eq!(
            intset_iter(&intset_build(2, &[-32_768, -1, 0, 5, 32_767])).unwrap(),
            vec![-32_768, -1, 0, 5, 32_767]
        );
        // 32-bit.
        assert_eq!(
            intset_iter(&intset_build(4, &[-2_000_000_000, 0, 2_000_000_000])).unwrap(),
            vec![-2_000_000_000, 0, 2_000_000_000]
        );
        // 64-bit.
        assert_eq!(
            intset_iter(&intset_build(8, &[i64::MIN, 0, i64::MAX])).unwrap(),
            vec![i64::MIN, 0, i64::MAX]
        );
        // Empty intset.
        assert_eq!(
            intset_iter(&intset_build(4, &[])).unwrap(),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn intset_rejects_a_non_ascending_blob() {
        // Descending / duplicate values are not a valid (sorted, unique) intset.
        let descending = intset_build(2, &[5, 3, 1]);
        assert_eq!(intset_iter(&descending), Err(RestoreParseError::BadData));
        let duplicate = intset_build(2, &[1, 1, 2]);
        assert_eq!(intset_iter(&duplicate), Err(RestoreParseError::BadData));
    }

    #[test]
    fn intset_rejects_a_wrong_total_length() {
        // A header length that does not match the actual payload byte count.
        let mut blob = intset_build(4, &[1, 2, 3]);
        blob[4..8].copy_from_slice(&5u32.to_le_bytes()); // claim 5 values, only 3 present
        assert_eq!(intset_iter(&blob), Err(RestoreParseError::BadData));
        // A bad encoding width.
        let mut bad_enc = intset_build(4, &[1, 2]);
        bad_enc[0..4].copy_from_slice(&3u32.to_le_bytes()); // encoding 3 is not in {2,4,8}
        assert_eq!(intset_iter(&bad_enc), Err(RestoreParseError::BadData));
    }

    #[test]
    fn intset_dos_a_huge_length_is_rejected_without_alloc() {
        // encoding=8, length claims ~2^32-1 elements: length*encoding vastly exceeds the slice (and the
        // value cap). Must reject cleanly with NO giant pre-allocation.
        let mut blob = Vec::new();
        blob.extend_from_slice(&8u32.to_le_bytes());
        blob.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        blob.extend_from_slice(&[0u8; 8]); // one token value's worth of bytes
        assert_eq!(intset_iter(&blob), Err(RestoreParseError::BadData));
    }

    // ---- RDB double (zset score) encodings. ----

    #[test]
    fn binary_double_reads_little_endian_and_the_non_finites() {
        // A finite value, its NEGATIVE, a fraction, and +inf/-inf all round-trip byte-for-byte through
        // the little-endian 8-byte reader; the reader returns the raw f64 (NaN gate is the caller's).
        for v in [3.5_f64, -2.0, 0.1, f64::INFINITY, f64::NEG_INFINITY] {
            let bytes = v.to_le_bytes();
            let mut pos = 0usize;
            let got = read_rdb_binary_double(&bytes, &mut pos).unwrap();
            assert_eq!(pos, 8);
            assert_eq!(got.to_bits(), v.to_bits(), "round trip for {v}");
        }
        // A NaN bit pattern is returned as-is (the caller rejects it).
        let mut pos = 0usize;
        assert!(
            read_rdb_binary_double(&f64::NAN.to_le_bytes(), &mut pos)
                .unwrap()
                .is_nan()
        );
        // A truncated tail (<8 bytes) is BadData, never an over-read.
        let mut pos = 0usize;
        assert_eq!(
            read_rdb_binary_double(&[0u8; 7], &mut pos),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn ascii_double_reads_sentinels_and_text() {
        // Sentinel lengths: 255 -> -inf, 254 -> +inf, 253 -> NaN.
        let mut pos = 0usize;
        assert_eq!(
            read_rdb_ascii_double(&[255], &mut pos),
            Ok(f64::NEG_INFINITY)
        );
        let mut pos = 0usize;
        assert_eq!(read_rdb_ascii_double(&[254], &mut pos), Ok(f64::INFINITY));
        let mut pos = 0usize;
        assert!(read_rdb_ascii_double(&[253], &mut pos).unwrap().is_nan());
        // A normal length-prefixed ASCII float: L then L bytes.
        let mut blob = vec![4u8];
        blob.extend_from_slice(b"3.25");
        let mut pos = 0usize;
        assert_eq!(read_rdb_ascii_double(&blob, &mut pos), Ok(3.25));
        assert_eq!(pos, 5);
        // A negative integer-valued score renders exactly.
        let mut blob = vec![2u8];
        blob.extend_from_slice(b"-7");
        let mut pos = 0usize;
        assert_eq!(read_rdb_ascii_double(&blob, &mut pos), Ok(-7.0));
        // A length that runs past the end is BadData, no over-read.
        let mut pos = 0usize;
        assert_eq!(
            read_rdb_ascii_double(&[9u8, b'1'], &mut pos),
            Err(RestoreParseError::BadData)
        );
        // Non-float text is BadData.
        let mut blob = vec![3u8];
        blob.extend_from_slice(b"1x2");
        let mut pos = 0usize;
        assert_eq!(
            read_rdb_ascii_double(&blob, &mut pos),
            Err(RestoreParseError::BadData)
        );
    }

    #[test]
    fn parse_ascii_double_accepts_inf_rejects_junk() {
        assert_eq!(parse_ascii_double(b"inf"), Ok(f64::INFINITY));
        assert_eq!(parse_ascii_double(b"-inf"), Ok(f64::NEG_INFINITY));
        assert_eq!(parse_ascii_double(b"1e10"), Ok(1e10));
        assert!(parse_ascii_double(b"nan").unwrap().is_nan());
        assert_eq!(parse_ascii_double(b""), Err(RestoreParseError::BadData));
        assert_eq!(parse_ascii_double(b"abc"), Err(RestoreParseError::BadData));
    }
}
