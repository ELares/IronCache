// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bitmap command handlers over the STRING type (BITMAPS.md, COMMANDS.md bitmap
//! semantics, ENCODINGS.md "a bitmap is a raw string"). PR-9: SETBIT, GETBIT,
//! BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO.
//!
//! ## A bitmap is the string type (no new ValueRepr)
//!
//! A bitmap is NOT a distinct data type in Redis: it is the string value addressed
//! at bit granularity, so TYPE returns `string` and OBJECT ENCODING reports a string
//! encoding (BITMAPS.md "bitmaps are the string type"). These handlers therefore
//! operate on [`DataType::String`] only and need NO new `ValueRepr`: a SETBIT on an
//! int- or embstr-encoded value treats its decimal/inline BYTES as the bitmap and
//! writes the result back as a RAW string.
//!
//! ## read vs rmw (the storage-waist mapping)
//!
//! - Reads (GETBIT, BITCOUNT, BITPOS, BITFIELD-all-GET, BITFIELD_RO) use
//!   [`Store::read`] (a borrow of the string bytes; absent/non-string handled per
//!   command).
//! - Mutations (SETBIT, BITOP-destination, BITFIELD with SET/INCRBY) use the existing
//!   [`Store::rmw`] path: the closure reads the old string bytes, modifies a
//!   `Vec<u8>`, and returns [`RmwAction::Replace`] with the new RAW bytes (or
//!   [`RmwAction::Delete`] when a BITOP result is empty). This is the same
//!   rebuild-and-Replace mechanism APPEND uses; it does NOT touch the core Store
//!   primitive signatures nor the in-place mechanism. The O(1)-amortized in-place
//!   SETBIT (an `as_string_mut` extension to the rmw-mut surface) is a documented
//!   EFFICIENCY follow-up (#8), NOT this PR.
//!
//! ## The bit-offset ceiling (no huge alloc)
//!
//! A bitmap is bounded by the string ceiling: 512 MB per value via
//! proto-max-bulk-len, so the maximum addressable bit offset is `4*1024*1024*1024-1`
//! (2^32 bits). SETBIT / BITFIELD at an offset that would grow the value past the
//! ceiling is REJECTED with the Redis-recognized "bit offset is not an integer or
//! out of range" error rather than allocating unboundedly (BITMAPS.md size ceiling).
//!
//! ## ENCODING DIVERGENCE (documented, identical to APPEND)
//!
//! Redis always reports `raw` for a SETBIT/BITOP/BITFIELD-written bitmap regardless of
//! its length. IronCache writes the rebuilt value back through the frozen waist's
//! `NewValueOwned::Bytes`, which the store classifies by LENGTH (ENCODINGS.md): a
//! result `<= EMBSTR_THRESHOLD` (44) bytes is therefore `embstr`, and only a result
//! over the threshold is `raw`. This is the SAME divergence `APPEND` documents and
//! accepts: forcing an always-`raw` result would require a new write-value variant on
//! the storage waist (a waist change, explicitly forbidden for this PR). The
//! length-classified encoding is behaviorally identical for every observable bit
//! operation (TYPE is always `string`; GETBIT/BITCOUNT/GET/STRLEN read the same bytes);
//! the in-place SETBIT efficiency path (#8) that would also fix this is the documented
//! follow-up. The logical type is ALWAYS `DataType::String` (a bitmap is the string
//! type), so the divergence is purely the embstr-vs-raw internal-representation name.

use crate::cmd_util::ascii_upper;
use bytes::Bytes;
use ironcache_protocol::{ErrorReply, Request, Value};
use ironcache_storage::{
    DataType, ExpireWrite, NewValueOwned, RmwAction, RmwEntry, RmwStep, Store, UnixMillis,
};

// ---------------------------------------------------------------------------
// The bit-offset ceiling (Redis proto-max-bit-offset, BITMAPS.md size ceiling).
// ---------------------------------------------------------------------------

/// The maximum addressable bit offset: `4*1024*1024*1024 - 1`. A SETBIT/BITFIELD
/// offset must satisfy `0 <= offset <= PROTO_MAX_BIT_OFFSET`; a byte length is bounded
/// by `(PROTO_MAX_BIT_OFFSET >> 3) + 1 == 512 MB`. This matches Redis's
/// proto-max-bit-offset (derived from the default `proto-max-bulk-len` of 512 MB), so
/// growing a bitmap past the ceiling is rejected rather than allocating 512 MB
/// unboundedly (BITMAPS.md "rejected with the Redis-recognized error, not truncated").
const PROTO_MAX_BIT_OFFSET: u64 = 4 * 1024 * 1024 * 1024 - 1;

// ---------------------------------------------------------------------------
// The bit-manipulation helper (get/set a bit MSB-first, popcount over byte/bit
// ranges, bitpos scan, the BITFIELD signed/unsigned field engine). Pure, panic-free,
// and unit-tested below. Bit 0 is the MOST SIGNIFICANT bit of byte 0 (big-endian
// within each byte), matching Redis [redis-setbit-zerofill-growth-bigendian].
// ---------------------------------------------------------------------------

/// The bit at absolute `offset` in `data` (MSB-first within each byte). Returns `0`
/// for any offset at or beyond the end of `data` (GETBIT past the string is 0).
fn get_bit(data: &[u8], offset: u64) -> u8 {
    let byte = (offset >> 3) as usize;
    let Some(&b) = data.get(byte) else {
        return 0;
    };
    // Within a byte, bit 0 is the MSB, so the shift is `7 - (offset & 7)`.
    let shift = 7 - (offset & 7) as u32;
    (b >> shift) & 1
}

/// Set the bit at absolute `offset` in `data` to `value` (0 or 1), zero-extending
/// `data` to fit the offset if needed (SETBIT zero-fill growth). Returns the OLD bit
/// value. The caller guarantees the offset is within [`PROTO_MAX_BIT_OFFSET`] (the
/// command handler checked the ceiling first, so this never grows unboundedly).
fn set_bit(data: &mut Vec<u8>, offset: u64, value: u8) -> u8 {
    let byte = (offset >> 3) as usize;
    if byte >= data.len() {
        // Zero-extend so the value length becomes (offset >> 3) + 1 bytes; the gap
        // reads as 0 (SETBIT zero-fill growth).
        data.resize(byte + 1, 0);
    }
    let shift = 7 - (offset & 7) as u32;
    let mask = 1u8 << shift;
    let old = (data[byte] >> shift) & 1;
    if value == 0 {
        data[byte] &= !mask;
    } else {
        data[byte] |= mask;
    }
    old
}

/// Count the set bits in the byte slice (whole-string BITCOUNT). A simple per-byte
/// popcount; the byte/bit-range variants narrow the slice first.
fn popcount(data: &[u8]) -> u64 {
    data.iter().map(|b| u64::from(b.count_ones())).sum()
}

/// Count the set bits in the inclusive BIT range `[start_bit, end_bit]` of `data`
/// (BITCOUNT ... BIT). Both bounds are already normalized to in-range bit indices
/// with `start_bit <= end_bit`; an empty/inverted range is handled by the caller.
fn popcount_bit_range(data: &[u8], start_bit: u64, end_bit: u64) -> u64 {
    let mut count = 0u64;
    // Walk whole bytes where possible; the partial first/last bytes are scanned bit
    // by bit. (A #8 follow-up could mask the edge bytes and popcount the middle in
    // one pass; the per-bit edge scan is correct and simple for v1.)
    let mut bit = start_bit;
    while bit <= end_bit {
        // If `bit` is byte-aligned (low 3 bits zero) and the whole byte is inside the
        // range, popcount it directly; otherwise scan the edge byte bit by bit.
        if bit.trailing_zeros() >= 3 && bit + 7 <= end_bit {
            let byte = (bit >> 3) as usize;
            count += u64::from(data[byte].count_ones());
            bit += 8;
        } else {
            count += u64::from(get_bit(data, bit));
            bit += 1;
        }
    }
    count
}

/// The position of the first `bit` (0 or 1) in `data` over the inclusive BIT range
/// `[start_bit, end_bit]`, or `None` if not found in that range. Both bounds are
/// in-range with `start_bit <= end_bit`.
fn bitpos_in_range(data: &[u8], target: u8, start_bit: u64, end_bit: u64) -> Option<u64> {
    let mut bit = start_bit;
    while bit <= end_bit {
        // Whole-byte fast skip: when scanning for a 1, skip an all-0 aligned byte;
        // when scanning for a 0, skip an all-1 (0xFF) aligned byte. `bit` is byte
        // -aligned when its low 3 bits are zero.
        if bit.trailing_zeros() >= 3 && bit + 7 <= end_bit {
            let byte = (bit >> 3) as usize;
            let b = data[byte];
            if (target == 1 && b == 0x00) || (target == 0 && b == 0xFF) {
                bit += 8;
                continue;
            }
        }
        if get_bit(data, bit) == target {
            return Some(bit);
        }
        bit += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Shared argument parsing.
// ---------------------------------------------------------------------------

/// Parse a base-10 i64 bit/field offset or value argument the way Redis's
/// `getLongLongFromObjectOrReply` does for the bit commands: a plain `[-]?digits`
/// (the looser form, NOT the strict no-leading-zeros rule the numeric RMW uses).
/// Returns `None` on any non-integer form (the caller maps `None` to the right error).
fn parse_i64(arg: &[u8]) -> Option<i64> {
    crate::cmd_util::parse_i64(arg)
}

// ---------------------------------------------------------------------------
// SETBIT / GETBIT.
// ---------------------------------------------------------------------------

/// `SETBIT key offset value` -> the OLD bit value (0/1). Creates/zero-extends the
/// string to fit `offset`; the result is a string value classified by length (raw over
/// the embstr threshold; see the ENCODING DIVERGENCE note in the module docs). `offset`
/// must be in `[0, PROTO_MAX_BIT_OFFSET]` ("bit offset is not an integer or out of
/// range" otherwise); `value` must be 0 or 1 ("bit is not an integer or out of range"
/// otherwise). WRONGTYPE on a non-string key. Mutation via the rmw rebuild-Replace
/// path. `denyoom` (it grows memory).
pub fn cmd_setbit<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 4 {
        return Value::error(ErrorReply::wrong_arity("setbit"));
    }
    // The offset: a non-negative integer within the proto-max-bit-offset ceiling.
    let Some(offset) = parse_bit_offset(&req.args[2]) else {
        return Value::error(ErrorReply::bit_offset_out_of_range());
    };
    // The value: exactly 0 or 1.
    let value: u8 = match req.args[3].as_ref() {
        b"0" => 0,
        b"1" => 1,
        _ => return Value::error(ErrorReply::bit_not_integer_or_range()),
    };
    store.rmw(db, &req.args[1], now, move |entry| match entry {
        RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
            keep_err(ErrorReply::wrong_type())
        }
        RmwEntry::Vacant => {
            // Create-on-write: a missing key starts as an empty string.
            let mut buf: Vec<u8> = Vec::new();
            let old = set_bit(&mut buf, offset, value);
            RmwStep {
                action: RmwAction::Insert(NewValueOwned::Bytes(Bytes::from(buf))),
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(i64::from(old)),
            }
        }
        RmwEntry::Occupied(o) => {
            let mut buf = o.as_bytes().to_vec();
            let old = set_bit(&mut buf, offset, value);
            RmwStep {
                // The result is a string value (length-classified embstr/raw; the module
                // docs note the divergence from Redis's always-raw). SETBIT does NOT
                // touch the TTL (Redis preserves the existing expire).
                action: RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(buf))),
                expire: ExpireWrite::Unchanged,
                reply: Value::Integer(i64::from(old)),
            }
        }
        // Unreachable: SETBIT uses the read-only `rmw`, never `rmw_mut`.
        RmwEntry::OccupiedMut(_) => unreachable!("cmd_setbit uses rmw, not rmw_mut"),
    })
}

/// `GETBIT key offset` -> the bit value (0/1); 0 if `offset` is at/beyond the string
/// or the key is missing. WRONGTYPE on a non-string. Read-only (via `read`).
pub fn cmd_getbit<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    if req.args.len() != 3 {
        return Value::error(ErrorReply::wrong_arity("getbit"));
    }
    let Some(offset) = parse_bit_offset(&req.args[2]) else {
        return Value::error(ErrorReply::bit_offset_out_of_range());
    };
    match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => {
            Value::Integer(i64::from(get_bit(v.as_bytes(), offset)))
        }
        Some(_) => Value::error(ErrorReply::wrong_type()),
        None => Value::Integer(0),
    }
}

/// Parse a SETBIT/GETBIT bit offset: a non-negative integer within the
/// proto-max-bit-offset ceiling. Returns `None` for a non-integer, a negative offset,
/// or one past the ceiling (the caller maps `None` to the bit-offset-out-of-range
/// error, which is what guards against a huge allocation).
fn parse_bit_offset(arg: &[u8]) -> Option<u64> {
    let n = parse_i64(arg)?;
    if n < 0 {
        return None;
    }
    let n = n as u64;
    if n > PROTO_MAX_BIT_OFFSET {
        return None;
    }
    Some(n)
}

// ---------------------------------------------------------------------------
// BITCOUNT.
// ---------------------------------------------------------------------------

/// `BITCOUNT key [start end [BYTE|BIT]]` -> the count of set bits. No range counts the
/// whole string; `start`/`end` are byte indices by default or bit indices with BIT
/// (Redis 7), negative-from-end and INCLUSIVE. A missing key is 0. WRONGTYPE on a
/// non-string. Read-only (via `read`).
pub fn cmd_bitcount<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // BITCOUNT key | BITCOUNT key start end | BITCOUNT key start end BYTE|BIT.
    if req.args.len() != 2 && req.args.len() != 4 && req.args.len() != 5 {
        return Value::error(ErrorReply::syntax_error());
    }
    // Parse the optional [start end [BYTE|BIT]] tail BEFORE the lookup so a malformed
    // range is a syntax/not-integer error regardless of the key (matching Redis).
    let range = match parse_count_range(req) {
        Ok(r) => r,
        Err(e) => return Value::error(e),
    };
    let data = match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => v.as_bytes().to_vec(),
        Some(_) => return Value::error(ErrorReply::wrong_type()),
        None => return Value::Integer(0),
    };
    match range {
        None => Value::Integer(popcount(&data) as i64),
        Some((start, end, unit)) => {
            let total_bits = (data.len() as u64) * 8;
            let (lo, hi) = match unit {
                RangeUnit::Byte => {
                    // Resolve byte indices, then convert to a bit range.
                    let Some((bs, be)) = resolve_range(start, end, data.len() as u64) else {
                        return Value::Integer(0);
                    };
                    (bs * 8, be * 8 + 7)
                }
                RangeUnit::Bit => {
                    let Some((bs, be)) = resolve_range(start, end, total_bits) else {
                        return Value::Integer(0);
                    };
                    (bs, be)
                }
            };
            Value::Integer(popcount_bit_range(&data, lo, hi) as i64)
        }
    }
}

/// The BYTE/BIT range unit for BITCOUNT/BITPOS (Redis 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeUnit {
    /// Byte indices (the default).
    Byte,
    /// Bit indices (the `BIT` keyword).
    Bit,
}

/// Parse the BITCOUNT `[start end [BYTE|BIT]]` tail. Returns `Ok(None)` for the
/// no-range form, `Ok(Some((start, end, unit)))` for a range, or an error (syntax for
/// a bad unit keyword, not-an-integer for a non-integer bound).
fn parse_count_range(req: &Request) -> Result<Option<(i64, i64, RangeUnit)>, ErrorReply> {
    if req.args.len() == 2 {
        return Ok(None);
    }
    let Some(start) = parse_i64(&req.args[2]) else {
        return Err(ErrorReply::not_an_integer());
    };
    let Some(end) = parse_i64(&req.args[3]) else {
        return Err(ErrorReply::not_an_integer());
    };
    let unit = if req.args.len() == 5 {
        match ascii_upper(&req.args[4]).as_slice() {
            b"BYTE" => RangeUnit::Byte,
            b"BIT" => RangeUnit::Bit,
            _ => return Err(ErrorReply::syntax_error()),
        }
    } else {
        RangeUnit::Byte
    };
    Ok(Some((start, end, unit)))
}

/// Normalize a signed inclusive Redis `[start, end]` range against `len` units (bytes
/// or bits) into an inclusive in-range `(lo, hi)` pair, or `None` if the range is
/// empty/inverted or `len` is 0. Negative indices count from the end; bounds are
/// clamped to `[0, len-1]` (Redis BITCOUNT/BITPOS range normalization).
fn resolve_range(start: i64, end: i64, len: u64) -> Option<(u64, u64)> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    let mut s = if start < 0 { start + len_i } else { start };
    let mut e = if end < 0 { end + len_i } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        // After adjusting, an end still below 0 means the range is entirely before the
        // start of the string -> empty.
        return None;
    }
    if e >= len_i {
        e = len_i - 1;
    }
    if s > e || s >= len_i {
        return None;
    }
    Some((s as u64, e as u64))
}

// ---------------------------------------------------------------------------
// BITPOS.
// ---------------------------------------------------------------------------

/// `BITPOS key bit [start [end [BYTE|BIT]]]` -> the position of the first 0/1 bit, or
/// -1 if not found. The edge cases match Redis: searching for a 1 not present returns
/// -1; searching for a 0 with NO explicit end on an all-1 string returns the first bit
/// PAST the string (the implicit trailing zero), whereas an explicit end bounds the
/// search to the stored bits. A missing key: bit 0 -> 0, bit 1 -> -1. WRONGTYPE on a
/// non-string. Read-only (via `read`).
pub fn cmd_bitpos<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // BITPOS key bit | + start | + start end | + start end BYTE|BIT.
    if !(3..=6).contains(&req.args.len()) {
        return Value::error(ErrorReply::syntax_error());
    }
    let target: u8 = match req.args[2].as_ref() {
        b"0" => 0,
        b"1" => 1,
        _ => return Value::error(ErrorReply::bit_not_integer_or_range()),
    };
    // Parse the optional [start [end [BYTE|BIT]]] tail before the lookup.
    let start_arg = if req.args.len() >= 4 {
        match parse_i64(&req.args[3]) {
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };
    let end_arg = if req.args.len() >= 5 {
        match parse_i64(&req.args[4]) {
            Some(n) => Some(n),
            None => return Value::error(ErrorReply::not_an_integer()),
        }
    } else {
        None
    };
    let unit = if req.args.len() == 6 {
        match ascii_upper(&req.args[5]).as_slice() {
            b"BYTE" => RangeUnit::Byte,
            b"BIT" => RangeUnit::Bit,
            _ => return Value::error(ErrorReply::syntax_error()),
        }
    } else {
        RangeUnit::Byte
    };

    let data = match store.read(db, &req.args[1], now) {
        Some(v) if v.data_type() == DataType::String => v.as_bytes().to_vec(),
        Some(_) => return Value::error(ErrorReply::wrong_type()),
        // A missing key is an empty string: searching for 0 -> 0, for 1 -> -1
        // (Redis: BITPOS on a non-existent key behaves like an empty string).
        None => return Value::Integer(if target == 0 { 0 } else { -1 }),
    };

    Value::Integer(bitpos(&data, target, start_arg, end_arg, unit))
}

/// The BITPOS core, factored out for unit testing. `start`/`end` are the optional raw
/// signed range bounds; `unit` selects BYTE vs BIT indexing. Implements the Redis edge
/// rules exactly (see [`cmd_bitpos`]).
fn bitpos(data: &[u8], target: u8, start: Option<i64>, end: Option<i64>, unit: RangeUnit) -> i64 {
    let total_bits = (data.len() as u64) * 8;
    if data.is_empty() {
        // An empty string: 0 is found at position 0 (the implicit trailing zero) only
        // when no explicit end bounds it; 1 is never found.
        return if target == 0 && end.is_none() { 0 } else { -1 };
    }
    // The default range is the whole string (start 0, end = last unit). Resolve the
    // explicit bounds into an inclusive bit range.
    let (lo, hi) = match unit {
        RangeUnit::Byte => {
            let s = start.unwrap_or(0);
            let e = end.unwrap_or((data.len() as i64) - 1);
            match resolve_range(s, e, data.len() as u64) {
                Some((bs, be)) => (bs * 8, be * 8 + 7),
                None => return -1,
            }
        }
        RangeUnit::Bit => {
            let s = start.unwrap_or(0);
            let e = end.unwrap_or((total_bits as i64) - 1);
            match resolve_range(s, e, total_bits) {
                Some((bs, be)) => (bs, be),
                None => return -1,
            }
        }
    };
    match bitpos_in_range(data, target, lo, hi) {
        Some(pos) => pos as i64,
        None => {
            // Not found in the range. The Redis special case: when searching for a 0
            // and NO explicit end was given, the bit just PAST the string counts as 0
            // (the implicit trailing zero), so return the first bit past the end.
            // With an explicit end the search is bounded and returns -1.
            if target == 0 && end.is_none() {
                total_bits as i64
            } else {
                -1
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BITOP.
// ---------------------------------------------------------------------------

/// `BITOP AND|OR|XOR|NOT destkey srckey [srckey ...]` -> the byte length of the
/// result. The op is applied across the source strings (shorter strings zero-padded to
/// the longest), stored in `destkey`; an empty result DELETES `destkey`. NOT takes
/// exactly one source. WRONGTYPE if any source is a non-string. SAME-SHARD only
/// (single-shard-per-connection, like the other multi-key commands: sources are read
/// on the accept shard and dest is written there). `denyoom` (it materializes a value
/// at the destination).
pub fn cmd_bitop<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    // BITOP op destkey srckey [srckey ...]: at least op + dest + one source.
    if req.args.len() < 4 {
        return Value::error(ErrorReply::wrong_arity("bitop"));
    }
    let op = ascii_upper(&req.args[1]);
    let is_not = op.as_slice() == b"NOT";
    if !matches!(op.as_slice(), b"AND" | b"OR" | b"XOR" | b"NOT") {
        return Value::error(ErrorReply::syntax_error());
    }
    // NOT takes EXACTLY one source key (BITOP NOT destkey srckey).
    if is_not && req.args.len() != 4 {
        return Value::error(ErrorReply::bitop_not_single_source());
    }
    let dest = req.args[1 + 1].clone();
    let src_keys: Vec<Bytes> = req.args[3..].to_vec();

    // Read every source string on the accept shard. A WRONGTYPE on any source aborts
    // with no write (checked before the dest mutation). A missing source contributes
    // an empty string (zero-padded to the longest in the op).
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
    for k in &src_keys {
        match store.read(db, k, now) {
            Some(v) if v.data_type() == DataType::String => sources.push(v.as_bytes().to_vec()),
            Some(_) => return Value::error(ErrorReply::wrong_type()),
            None => sources.push(Vec::new()),
        }
    }

    let result = bitop_compute(op.as_slice(), &sources);

    if result.is_empty() {
        // An empty result deletes the destination key (Redis BITOP deletes dest).
        store.delete(db, &dest, now);
    } else {
        let len = result.len();
        let bytes = Bytes::from(result);
        store.rmw(db, &dest, now, move |_entry| RmwStep {
            // Overwrite dest with the RAW result (a bitmap is a raw string). The dest
            // of a BITOP has no TTL (Redis clears the dest TTL on a STORE-like write).
            action: RmwAction::Replace(NewValueOwned::Bytes(bytes)),
            expire: ExpireWrite::Clear,
            reply: (),
        });
        return Value::Integer(len as i64);
    }
    Value::Integer(0)
}

/// Compute the BITOP result bytes over `sources` (already materialized, missing keys
/// as empty vecs). The result length equals the LONGEST source (shorter sources
/// zero-padded); NOT inverts the single source to an equal-length result. Factored out
/// for unit testing.
fn bitop_compute(op: &[u8], sources: &[Vec<u8>]) -> Vec<u8> {
    if op == b"NOT" {
        // NOT: exactly one source (the caller enforced arity); invert every byte.
        let src = sources.first().map_or(&[][..], Vec::as_slice);
        return src.iter().map(|b| !b).collect();
    }
    let max_len = sources.iter().map(Vec::len).max().unwrap_or(0);
    if max_len == 0 {
        return Vec::new();
    }
    let mut out = vec![0u8; max_len];
    for (i, byte) in out.iter_mut().enumerate() {
        // Seed from the FIRST source (zero-padded), then fold the rest.
        let mut acc = sources.first().and_then(|s| s.get(i)).copied().unwrap_or(0);
        for src in &sources[1..] {
            let b = src.get(i).copied().unwrap_or(0);
            acc = match op {
                b"AND" => acc & b,
                b"OR" => acc | b,
                b"XOR" => acc ^ b,
                // Unreachable: the caller validated op as AND/OR/XOR/NOT and handled NOT.
                _ => acc,
            };
        }
        *byte = acc;
    }
    out
}

// ---------------------------------------------------------------------------
// BITFIELD / BITFIELD_RO.
// ---------------------------------------------------------------------------

/// A parsed BITFIELD integer field type: signed `i<N>` (1..=64) or unsigned `u<N>`
/// (1..=63), per Redis (`u64` is not supported, `i64` is).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FieldType {
    /// Whether the field is signed (`i`) vs unsigned (`u`).
    signed: bool,
    /// The bit width: 1..=64 for signed, 1..=63 for unsigned.
    bits: u32,
}

impl FieldType {
    /// Parse an `i<N>` / `u<N>` type token, or `None` on a bad form / out-of-range
    /// width (the caller maps `None` to the invalid-bitfield-type error).
    fn parse(arg: &[u8]) -> Option<FieldType> {
        let (signed, rest) = match arg.first()? {
            b'i' | b'I' => (true, &arg[1..]),
            b'u' | b'U' => (false, &arg[1..]),
            _ => return None,
        };
        if rest.is_empty() {
            return None;
        }
        let mut bits: u32 = 0;
        for &b in rest {
            if !b.is_ascii_digit() {
                return None;
            }
            bits = bits.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
        }
        // Signed up to 64 bits; unsigned up to 63 bits (Redis width limits).
        if bits == 0 {
            return None;
        }
        if signed && bits > 64 {
            return None;
        }
        if !signed && bits > 63 {
            return None;
        }
        Some(FieldType { signed, bits })
    }

    /// The inclusive maximum representable value (as i128 to hold u63 and i64 alike).
    fn max(self) -> i128 {
        if self.signed {
            (1i128 << (self.bits - 1)) - 1
        } else {
            (1i128 << self.bits) - 1
        }
    }

    /// The inclusive minimum representable value.
    fn min(self) -> i128 {
        if self.signed {
            -(1i128 << (self.bits - 1))
        } else {
            0
        }
    }
}

/// The OVERFLOW mode for BITFIELD SET/INCRBY (applies to subsequent write ops in the
/// same command; default WRAP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overflow {
    /// Modular two's-complement wraparound (the default).
    Wrap,
    /// Saturate to the type min/max.
    Sat,
    /// Leave the field unchanged and return a null for that op.
    Fail,
}

/// One parsed BITFIELD sub-operation.
#[derive(Debug, Clone)]
enum BitfieldOp {
    /// GET type offset.
    Get { ty: FieldType, offset: u64 },
    /// SET type offset value (with the OVERFLOW mode active at this op).
    Set {
        ty: FieldType,
        offset: u64,
        value: i128,
        overflow: Overflow,
    },
    /// INCRBY type offset increment (with the OVERFLOW mode active at this op).
    Incrby {
        ty: FieldType,
        offset: u64,
        incr: i128,
        overflow: Overflow,
    },
}

/// `BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset value]
/// [OVERFLOW WRAP|SAT|FAIL]...` -> an array (one element per GET/SET/INCRBY). SET
/// returns the OLD value; INCRBY the new value (or nil under OVERFLOW FAIL). Mutations
/// use the rmw rebuild-Replace path. `denyoom` when it has any write op. WRONGTYPE on a
/// non-string.
pub fn cmd_bitfield<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    bitfield_generic(store, db, now, req, false)
}

/// `BITFIELD_RO key [GET type offset]...` -> the read-only variant: only GET is
/// allowed; SET/INCRBY are an error. Always uses `read` (no write).
pub fn cmd_bitfield_ro<S: Store>(store: &mut S, db: u32, now: UnixMillis, req: &Request) -> Value {
    bitfield_generic(store, db, now, req, true)
}

/// Shared body for BITFIELD / BITFIELD_RO. `read_only` rejects SET/INCRBY with the
/// Redis BITFIELD_RO error. Parses ALL sub-ops first (so a parse error reports before
/// any mutation), then runs them as ONE atomic command over the value: reads go through
/// `read` when there is no write op; otherwise one `rmw` observes-modifies-Replaces.
fn bitfield_generic<S: Store>(
    store: &mut S,
    db: u32,
    now: UnixMillis,
    req: &Request,
    read_only: bool,
) -> Value {
    let cmd_name = if read_only { "bitfield_ro" } else { "bitfield" };
    if req.args.len() < 2 {
        return Value::error(ErrorReply::wrong_arity(cmd_name));
    }
    let ops = match parse_bitfield_ops(&req.args[2..], read_only) {
        Ok(ops) => ops,
        Err(e) => return Value::error(e),
    };

    // Whether any op writes (drives read vs rmw). BITFIELD_RO never writes.
    let has_write = ops
        .iter()
        .any(|op| matches!(op, BitfieldOp::Set { .. } | BitfieldOp::Incrby { .. }));

    if !has_write {
        // All-GET (or BITFIELD_RO): a borrow of the string bytes is enough.
        let data = match store.read(db, &req.args[1], now) {
            Some(v) if v.data_type() == DataType::String => v.as_bytes().to_vec(),
            Some(_) => return Value::error(ErrorReply::wrong_type()),
            None => Vec::new(),
        };
        let results = run_bitfield_ops(&mut data.clone(), &ops);
        return Value::Array(Some(results));
    }

    // Has at least one write: one atomic rmw observes the old bytes, runs every op on
    // the working buffer, and writes the result back as a RAW string.
    store.rmw(db, &req.args[1], now, move |entry| {
        let mut buf = match &entry {
            RmwEntry::Occupied(o) if o.data_type() != DataType::String => {
                return keep_err(ErrorReply::wrong_type());
            }
            RmwEntry::Occupied(o) => o.as_bytes().to_vec(),
            RmwEntry::Vacant => Vec::new(),
            // Unreachable: BITFIELD uses the read-only `rmw`, never `rmw_mut`.
            RmwEntry::OccupiedMut(_) => unreachable!("cmd_bitfield uses rmw, not rmw_mut"),
        };
        let results = run_bitfield_ops(&mut buf, &ops);
        // If the value is now empty (only possible when the key was absent and every
        // op was a no-op GET-style, which cannot happen here since has_write is true and
        // a write grows the buffer), delete; otherwise write the RAW result. A write op
        // always touches at least one byte, so `buf` is non-empty here.
        let action = if buf.is_empty() {
            RmwAction::Delete
        } else {
            RmwAction::Replace(NewValueOwned::Bytes(Bytes::from(buf)))
        };
        RmwStep {
            action,
            // BITFIELD does NOT touch the TTL (Redis preserves the existing expire).
            expire: ExpireWrite::Unchanged,
            reply: Value::Array(Some(results)),
        }
    })
}

/// Parse the BITFIELD sub-op tail into a vector of [`BitfieldOp`]. OVERFLOW sets the
/// mode for the following SET/INCRBY ops (default WRAP). `read_only` rejects SET/INCRBY
/// (and OVERFLOW, which is meaningless without a write) with the BITFIELD_RO error.
fn parse_bitfield_ops(args: &[Bytes], read_only: bool) -> Result<Vec<BitfieldOp>, ErrorReply> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 0;
    while i < args.len() {
        let kw = ascii_upper(&args[i]);
        match kw.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                let ty =
                    FieldType::parse(&args[i + 1]).ok_or_else(ErrorReply::invalid_bitfield_type)?;
                let offset = parse_field_offset(&args[i + 2], ty)?;
                ops.push(BitfieldOp::Get { ty, offset });
                i += 3;
            }
            b"SET" => {
                if read_only {
                    return Err(ErrorReply::bitfield_ro_no_writes());
                }
                if i + 3 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                let ty =
                    FieldType::parse(&args[i + 1]).ok_or_else(ErrorReply::invalid_bitfield_type)?;
                let offset = parse_field_offset(&args[i + 2], ty)?;
                let value =
                    i128::from(parse_i64(&args[i + 3]).ok_or_else(ErrorReply::not_an_integer)?);
                ops.push(BitfieldOp::Set {
                    ty,
                    offset,
                    value,
                    overflow,
                });
                i += 4;
            }
            b"INCRBY" => {
                if read_only {
                    return Err(ErrorReply::bitfield_ro_no_writes());
                }
                if i + 3 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                let ty =
                    FieldType::parse(&args[i + 1]).ok_or_else(ErrorReply::invalid_bitfield_type)?;
                let offset = parse_field_offset(&args[i + 2], ty)?;
                let incr =
                    i128::from(parse_i64(&args[i + 3]).ok_or_else(ErrorReply::not_an_integer)?);
                ops.push(BitfieldOp::Incrby {
                    ty,
                    offset,
                    incr,
                    overflow,
                });
                i += 4;
            }
            b"OVERFLOW" => {
                if read_only {
                    return Err(ErrorReply::bitfield_ro_no_writes());
                }
                if i + 1 >= args.len() {
                    return Err(ErrorReply::syntax_error());
                }
                overflow = match ascii_upper(&args[i + 1]).as_slice() {
                    b"WRAP" => Overflow::Wrap,
                    b"SAT" => Overflow::Sat,
                    b"FAIL" => Overflow::Fail,
                    _ => return Err(ErrorReply::bitfield_invalid_overflow()),
                };
                i += 2;
            }
            _ => return Err(ErrorReply::syntax_error()),
        }
    }
    Ok(ops)
}

/// Parse a BITFIELD offset: a plain bit offset, or `#<n>` for `n * type-width` bits.
/// Bounded by the proto-max-bit-offset ceiling (the field's HIGHEST bit must fit), so a
/// huge offset is the bit-offset-out-of-range error rather than a huge allocation.
fn parse_field_offset(arg: &[u8], ty: FieldType) -> Result<u64, ErrorReply> {
    let bits = if let Some(rest) = arg.strip_prefix(b"#") {
        // `#n` means n * type-width bits.
        let n = parse_i64(rest).ok_or_else(ErrorReply::bit_offset_out_of_range)?;
        if n < 0 {
            return Err(ErrorReply::bit_offset_out_of_range());
        }
        (n as u64).checked_mul(u64::from(ty.bits))
    } else {
        let n = parse_i64(arg).ok_or_else(ErrorReply::bit_offset_out_of_range)?;
        if n < 0 {
            return Err(ErrorReply::bit_offset_out_of_range());
        }
        Some(n as u64)
    };
    let bits = bits.ok_or_else(ErrorReply::bit_offset_out_of_range)?;
    // The field's HIGHEST bit is offset + width - 1; it must fit the ceiling so the
    // backing string never grows past 512 MB.
    let highest = bits
        .checked_add(u64::from(ty.bits) - 1)
        .ok_or_else(ErrorReply::bit_offset_out_of_range)?;
    if highest > PROTO_MAX_BIT_OFFSET {
        return Err(ErrorReply::bit_offset_out_of_range());
    }
    Ok(bits)
}

/// Run the parsed BITFIELD ops over the working buffer `buf` (a `Vec<u8>` the caller
/// will write back), returning the reply array (one [`Value`] per op). GET reads the
/// field; SET writes the new value and returns the OLD; INCRBY applies the increment
/// under the op's overflow mode and returns the new value (or nil under FAIL). A
/// field beyond the current end grows + zero-fills the buffer exactly as SETBIT does.
fn run_bitfield_ops(buf: &mut Vec<u8>, ops: &[BitfieldOp]) -> Vec<Value> {
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        match *op {
            BitfieldOp::Get { ty, offset } => {
                out.push(Value::Integer(get_field(buf, offset, ty) as i64));
            }
            BitfieldOp::Set {
                ty,
                offset,
                value,
                overflow,
            } => {
                let old = get_field(buf, offset, ty);
                match clamp_overflow(value, ty, overflow) {
                    Some(v) => {
                        set_field(buf, offset, ty, v);
                        out.push(Value::Integer(old as i64));
                    }
                    None => {
                        // FAIL: leave the field unchanged and return nil for this op.
                        out.push(Value::Null);
                    }
                }
            }
            BitfieldOp::Incrby {
                ty,
                offset,
                incr,
                overflow,
            } => {
                let cur = get_field(buf, offset, ty);
                // The wide sum cannot overflow i128 (both terms fit i64/u63 magnitudes).
                let sum = cur + incr;
                match clamp_overflow(sum, ty, overflow) {
                    Some(v) => {
                        set_field(buf, offset, ty, v);
                        out.push(Value::Integer(v as i64));
                    }
                    None => out.push(Value::Null),
                }
            }
        }
    }
    out
}

/// Apply the OVERFLOW mode to a candidate field `value` for type `ty`. Returns the
/// value to store (`Some`), or `None` when OVERFLOW FAIL rejects an out-of-range result
/// (the caller leaves the field unchanged and replies nil). WRAP wraps modulo `2^bits`
/// into the type's signed/unsigned range; SAT saturates to min/max; an in-range value
/// is returned unchanged by every mode.
fn clamp_overflow(value: i128, ty: FieldType, overflow: Overflow) -> Option<i128> {
    let min = ty.min();
    let max = ty.max();
    if value >= min && value <= max {
        return Some(value);
    }
    match overflow {
        Overflow::Fail => None,
        Overflow::Sat => Some(if value > max { max } else { min }),
        Overflow::Wrap => {
            // Wrap modulo 2^bits, then re-interpret in the type's range. The modulus
            // fits i128 for bits <= 64.
            let modulus = 1i128 << ty.bits;
            let mut wrapped = value % modulus;
            if wrapped < 0 {
                wrapped += modulus;
            }
            // `wrapped` is now in [0, 2^bits). For a signed type, values at or above
            // 2^(bits-1) represent negatives.
            if ty.signed && wrapped >= (1i128 << (ty.bits - 1)) {
                wrapped -= modulus;
            }
            Some(wrapped)
        }
    }
}

/// Read the integer field of width `ty.bits` at bit `offset` from `buf` (MSB-first,
/// big-endian within and across bytes), sign-extending a signed type. Bits past the end
/// of `buf` read as 0 (a field beyond the string reads as 0).
fn get_field(buf: &[u8], offset: u64, ty: FieldType) -> i128 {
    let mut raw: u128 = 0;
    for k in 0..ty.bits {
        raw = (raw << 1) | u128::from(get_bit(buf, offset + u64::from(k)));
    }
    if ty.signed && ty.bits > 0 && (raw >> (ty.bits - 1)) & 1 == 1 {
        // Sign-extend: subtract 2^bits.
        (raw as i128) - (1i128 << ty.bits)
    } else {
        raw as i128
    }
}

/// Write the integer field of width `ty.bits` at bit `offset` into `buf` (MSB-first),
/// zero-extending `buf` to fit the field's highest bit (a field beyond the end grows +
/// zero-fills exactly as SETBIT). `value` is already clamped into the type's range.
fn set_field(buf: &mut Vec<u8>, offset: u64, ty: FieldType, value: i128) {
    // Mask to the field width (two's-complement low bits).
    let mask: u128 = if ty.bits >= 128 {
        u128::MAX
    } else {
        (1u128 << ty.bits) - 1
    };
    let raw = (value as u128) & mask;
    for k in 0..ty.bits {
        // Bit k from the MSB side of the field.
        let bit = ((raw >> (ty.bits - 1 - k)) & 1) as u8;
        set_bit(buf, offset + u64::from(k), bit);
    }
}

// ---------------------------------------------------------------------------
// Shared rmw abort helper.
// ---------------------------------------------------------------------------

/// A no-write rmw step that just returns an error reply (value + TTL untouched). The
/// shared abort path for the bitmap mutators (WRONGTYPE etc.).
fn keep_err(e: ErrorReply) -> RmwStep<Value> {
    RmwStep {
        action: RmwAction::Keep,
        expire: ExpireWrite::Unchanged,
        reply: Value::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_storage::{CountingAccounting, Encoding, Store};
    use ironcache_store::ShardStore;

    type TestStore = ShardStore<ironcache_eviction::Policy, CountingAccounting>;

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

    const NOW: UnixMillis = UnixMillis(0);

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    fn err_line(v: &Value) -> String {
        match v {
            Value::Error(e) => e.line(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    /// Read the raw value bytes at `key` (decimal for an int, inline/raw for a string).
    fn get_bytes(store: &mut TestStore, key: &[u8]) -> Option<Vec<u8>> {
        store.read(0, key, NOW).map(|v| v.as_bytes().to_vec())
    }

    fn get_encoding(store: &mut TestStore, key: &[u8]) -> Option<Encoding> {
        store.read(0, key, NOW).map(|v| v.encoding())
    }

    // ---- Bit helper: get/set MSB-first, zero-extend, old-bit return. ----

    #[test]
    fn get_bit_is_msb_first_and_zero_past_end() {
        // 0b1000_0000 = 0x80: bit 0 (the MSB) is 1, bits 1..7 are 0.
        let data = [0x80u8];
        assert_eq!(get_bit(&data, 0), 1);
        for i in 1..8 {
            assert_eq!(get_bit(&data, i), 0, "bit {i}");
        }
        // 0x01: only bit 7 (the LSB) is set.
        let data = [0x01u8];
        assert_eq!(get_bit(&data, 7), 1);
        assert_eq!(get_bit(&data, 6), 0);
        // Past the end reads 0 without growing.
        assert_eq!(get_bit(&data, 8), 0);
        assert_eq!(get_bit(&data, 1_000_000), 0);
    }

    #[test]
    fn set_bit_zero_extends_and_returns_old_bit() {
        let mut buf: Vec<u8> = Vec::new();
        // Setting bit 7 on an empty buffer extends to 1 byte and returns old 0.
        assert_eq!(set_bit(&mut buf, 7, 1), 0);
        assert_eq!(buf, vec![0x01]);
        // Re-setting it returns the old 1.
        assert_eq!(set_bit(&mut buf, 7, 1), 1);
        // Setting a far bit zero-fills the gap: bit 100 -> byte 12 (100>>3), len 13.
        assert_eq!(set_bit(&mut buf, 100, 1), 0);
        assert_eq!(buf.len(), 13);
        assert_eq!(get_bit(&buf, 100), 1);
        // The gap bits all read 0.
        for i in 8..100 {
            assert_eq!(get_bit(&buf, i), 0, "gap bit {i}");
        }
        // Clearing a bit returns its old value and zeroes it.
        assert_eq!(set_bit(&mut buf, 100, 0), 1);
        assert_eq!(get_bit(&buf, 100), 0);
    }

    // ---- Bit helper: popcount over byte AND bit ranges. ----

    #[test]
    fn popcount_whole_and_ranges() {
        // 0xFF 0x0F: 8 + 4 = 12 set bits.
        let data = [0xFFu8, 0x0F];
        assert_eq!(popcount(&data), 12);
        // BIT range covering only the first byte (bits 0..=7) -> 8.
        assert_eq!(popcount_bit_range(&data, 0, 7), 8);
        // BIT range bits 8..=15 (second byte 0x0F) -> 4.
        assert_eq!(popcount_bit_range(&data, 8, 15), 4);
        // BIT range straddling a byte boundary: bits 4..=11.
        // byte0 0xFF bits 4..7 = 4 set; byte1 0x0F bits 8..11 = 0 set (high nibble) -> 4.
        assert_eq!(popcount_bit_range(&data, 4, 11), 4);
        // A single bit range.
        assert_eq!(popcount_bit_range(&data, 0, 0), 1);
        assert_eq!(popcount_bit_range(&data, 15, 15), 1);
    }

    // ---- Bit helper: bitpos incl. all-1s-find-0 and not-found edges. ----

    #[test]
    fn bitpos_finds_first_set_and_clear() {
        // 0x0F 0xF0: first 1 is at bit 4.
        let data = [0x0Fu8, 0xF0];
        assert_eq!(bitpos(&data, 1, None, None, RangeUnit::Byte), 4);
        // First 0 is at bit 0.
        assert_eq!(bitpos(&data, 0, None, None, RangeUnit::Byte), 0);
    }

    #[test]
    fn bitpos_find_one_not_found_returns_minus_one() {
        // All-0 string: searching for 1 returns -1.
        let data = [0x00u8, 0x00];
        assert_eq!(bitpos(&data, 1, None, None, RangeUnit::Byte), -1);
    }

    #[test]
    fn bitpos_find_zero_in_all_ones_no_end_returns_past_string() {
        // All-1 string, searching for 0 with NO explicit end -> the first bit PAST the
        // string (the implicit trailing zero). 2 bytes = 16 bits -> position 16.
        let data = [0xFFu8, 0xFF];
        assert_eq!(bitpos(&data, 0, None, None, RangeUnit::Byte), 16);
        // With an explicit end, the search is bounded to the stored bits -> -1.
        assert_eq!(bitpos(&data, 0, Some(0), Some(-1), RangeUnit::Byte), -1);
    }

    #[test]
    fn bitpos_bit_unit_range() {
        // 0x00 0x80: the only set bit is bit 8 (the MSB of byte 1).
        let data = [0x00u8, 0x80];
        assert_eq!(bitpos(&data, 1, None, None, RangeUnit::Byte), 8);
        // BIT-unit range starting at bit 8 still finds it.
        assert_eq!(bitpos(&data, 1, Some(8), Some(15), RangeUnit::Bit), 8);
        // BIT-unit range starting at bit 9 misses it.
        assert_eq!(bitpos(&data, 1, Some(9), Some(15), RangeUnit::Bit), -1);
    }

    // ---- Bit helper: BITFIELD field get/set/incrby + overflow. ----

    #[test]
    fn bitfield_get_set_unsigned_and_signed() {
        let mut buf: Vec<u8> = Vec::new();
        let u8t = FieldType {
            signed: false,
            bits: 8,
        };
        // SET u8 at offset 0 -> store 200; the field reads back 200.
        set_field(&mut buf, 0, u8t, 200);
        assert_eq!(get_field(&buf, 0, u8t), 200);
        // A signed i8 view of the same bits reads -56 (200 - 256).
        let i8t = FieldType {
            signed: true,
            bits: 8,
        };
        assert_eq!(get_field(&buf, 0, i8t), -56);
        // A field past the end reads 0.
        assert_eq!(get_field(&buf, 64, u8t), 0);
    }

    #[test]
    fn bitfield_overflow_wrap_sat_fail() {
        let u8t = FieldType {
            signed: false,
            bits: 8,
        };
        // u8 max is 255. 255 + 10 = 265.
        // WRAP: 265 mod 256 = 9.
        assert_eq!(clamp_overflow(265, u8t, Overflow::Wrap), Some(9));
        // SAT: saturates to 255.
        assert_eq!(clamp_overflow(265, u8t, Overflow::Sat), Some(255));
        // FAIL: rejected (None).
        assert_eq!(clamp_overflow(265, u8t, Overflow::Fail), None);

        let i8t = FieldType {
            signed: true,
            bits: 8,
        };
        // i8 range is [-128, 127]. 127 + 1 = 128.
        // WRAP: 128 wraps to -128.
        assert_eq!(clamp_overflow(128, i8t, Overflow::Wrap), Some(-128));
        // SAT: 127.
        assert_eq!(clamp_overflow(128, i8t, Overflow::Sat), Some(127));
        // Underflow: -128 - 1 = -129. WRAP -> 127; SAT -> -128; FAIL -> None.
        assert_eq!(clamp_overflow(-129, i8t, Overflow::Wrap), Some(127));
        assert_eq!(clamp_overflow(-129, i8t, Overflow::Sat), Some(-128));
        assert_eq!(clamp_overflow(-129, i8t, Overflow::Fail), None);
        // An in-range value is unchanged by every mode.
        assert_eq!(clamp_overflow(5, i8t, Overflow::Fail), Some(5));
    }

    #[test]
    fn field_type_parse_widths_and_limits() {
        assert_eq!(
            FieldType::parse(b"u8"),
            Some(FieldType {
                signed: false,
                bits: 8
            })
        );
        assert_eq!(
            FieldType::parse(b"i64"),
            Some(FieldType {
                signed: true,
                bits: 64
            })
        );
        assert_eq!(
            FieldType::parse(b"u63"),
            Some(FieldType {
                signed: false,
                bits: 63
            })
        );
        // u64 is NOT supported; i64 is. u0/i0 invalid. i65 invalid.
        assert_eq!(FieldType::parse(b"u64"), None);
        assert_eq!(FieldType::parse(b"i65"), None);
        assert_eq!(FieldType::parse(b"u0"), None);
        assert_eq!(FieldType::parse(b"i0"), None);
        assert_eq!(FieldType::parse(b"x8"), None);
        assert_eq!(FieldType::parse(b"i"), None);
        assert_eq!(FieldType::parse(b""), None);
    }

    // ---- SETBIT: old-bit return + extend + creates-raw. ----

    #[test]
    fn setbit_returns_old_bit_extends_and_creates_string() {
        let mut s = test_store();
        // Create-on-write: SETBIT on a missing key returns old 0.
        assert_eq!(
            int(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"7", b"1"])
            )),
            0
        );
        // The value is the single byte 0x01. TYPE is always string; the 1-byte result is
        // length-classified embstr (the documented divergence from Redis's always-raw).
        assert_eq!(get_bytes(&mut s, b"k"), Some(vec![0x01]));
        assert_eq!(s.type_of(0, b"k", NOW), Some(DataType::String));
        assert_eq!(get_encoding(&mut s, b"k"), Some(Encoding::EmbStr));
        // Re-set returns the old 1.
        assert_eq!(
            int(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"7", b"1"])
            )),
            1
        );
        // Setting a far bit extends + zero-fills.
        assert_eq!(
            int(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"100", b"1"])
            )),
            0
        );
        assert_eq!(get_bytes(&mut s, b"k").unwrap().len(), 13);
    }

    #[test]
    fn setbit_over_embstr_threshold_is_raw() {
        let mut s = test_store();
        // Setting a bit in byte 50 grows the value to 51 bytes, over the 44-byte embstr
        // threshold, so it is classified raw (matching Redis's always-raw for the
        // not-short case). bit 400 -> byte 50, len 51.
        assert_eq!(
            int(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"400", b"1"])
            )),
            0
        );
        assert_eq!(get_bytes(&mut s, b"k").unwrap().len(), 51);
        assert_eq!(get_encoding(&mut s, b"k"), Some(Encoding::Raw));
        assert_eq!(s.type_of(0, b"k", NOW), Some(DataType::String));
    }

    #[test]
    fn setbit_value_and_offset_errors() {
        let mut s = test_store();
        // Value must be 0 or 1.
        assert_eq!(
            err_line(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"0", b"2"])
            )),
            "-ERR bit is not an integer or out of range"
        );
        // A negative offset is the bit-offset-out-of-range error.
        assert_eq!(
            err_line(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"-1", b"1"])
            )),
            "-ERR bit offset is not an integer or out of range"
        );
        // A non-integer offset.
        assert_eq!(
            err_line(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", b"abc", b"1"])
            )),
            "-ERR bit offset is not an integer or out of range"
        );
    }

    #[test]
    fn setbit_at_2pow32_offset_is_rejected_no_huge_alloc() {
        let mut s = test_store();
        // 2^32 is one past PROTO_MAX_BIT_OFFSET (4*1024*1024*1024 - 1): rejected with no
        // allocation rather than growing a 512 MB value.
        let off = (4u64 * 1024 * 1024 * 1024).to_string();
        assert_eq!(
            err_line(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"k", off.as_bytes(), b"1"])
            )),
            "-ERR bit offset is not an integer or out of range"
        );
        // The key was never created.
        assert_eq!(get_bytes(&mut s, b"k"), None);
        // The MAXIMUM legal offset (2^32 - 1) is accepted but we must not run it here (it
        // would allocate 512 MB); the boundary check is covered by parse_bit_offset unit
        // logic: PROTO_MAX_BIT_OFFSET is exactly 2^32 - 1.
        assert_eq!(PROTO_MAX_BIT_OFFSET, 4 * 1024 * 1024 * 1024 - 1);
    }

    // ---- GETBIT: beyond-string = 0, missing = 0, WRONGTYPE. ----

    #[test]
    fn getbit_reads_bits_and_zero_beyond() {
        let mut s = test_store();
        cmd_setbit(&mut s, 0, NOW, &req(&[b"SETBIT", b"k", b"7", b"1"]));
        assert_eq!(
            int(&cmd_getbit(&mut s, 0, NOW, &req(&[b"GETBIT", b"k", b"7"]))),
            1
        );
        assert_eq!(
            int(&cmd_getbit(&mut s, 0, NOW, &req(&[b"GETBIT", b"k", b"0"]))),
            0
        );
        // Beyond the string -> 0.
        assert_eq!(
            int(&cmd_getbit(
                &mut s,
                0,
                NOW,
                &req(&[b"GETBIT", b"k", b"100"])
            )),
            0
        );
        // Missing key -> 0.
        assert_eq!(
            int(&cmd_getbit(
                &mut s,
                0,
                NOW,
                &req(&[b"GETBIT", b"missing", b"3"])
            )),
            0
        );
    }

    // ---- BITCOUNT: whole + BYTE range + BIT range + missing-key. ----

    #[test]
    fn bitcount_whole_byte_and_bit_ranges() {
        let mut s = test_store();
        // "foobar" has 26 set bits (the canonical Redis BITCOUNT example).
        store_string(&mut s, b"k", b"foobar");
        assert_eq!(
            int(&cmd_bitcount(&mut s, 0, NOW, &req(&[b"BITCOUNT", b"k"]))),
            26
        );
        // BYTE range [1,1] ('o') has 6 set bits; [0,0] ('f') has 4.
        assert_eq!(
            int(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"1", b"1"])
            )),
            6
        );
        assert_eq!(
            int(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"0", b"0"])
            )),
            4
        );
        // BYTE range [0,0] with explicit BYTE keyword matches.
        assert_eq!(
            int(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"0", b"0", b"BYTE"])
            )),
            4
        );
        // BIT range [5,30] (the Redis 7 example) -> 17.
        assert_eq!(
            int(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"5", b"30", b"BIT"])
            )),
            17
        );
        // Missing key -> 0.
        assert_eq!(
            int(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"missing"])
            )),
            0
        );
    }

    #[test]
    fn bitcount_bad_unit_and_arity() {
        let mut s = test_store();
        store_string(&mut s, b"k", b"foobar");
        // A bad unit keyword is a syntax error.
        assert_eq!(
            err_line(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"0", b"0", b"NOPE"])
            )),
            "-ERR syntax error"
        );
        // start/end without both bounds (3 args) is a syntax error.
        assert_eq!(
            err_line(&cmd_bitcount(
                &mut s,
                0,
                NOW,
                &req(&[b"BITCOUNT", b"k", b"0"])
            )),
            "-ERR syntax error"
        );
    }

    // ---- BITPOS: bit 0/1 incl edges + missing key. ----

    #[test]
    fn bitpos_command_edges() {
        let mut s = test_store();
        // 0xFF 0xF0 0x00.
        store_string(&mut s, b"k", &[0xFF, 0xF0, 0x00]);
        // First 0 bit is at position 12.
        assert_eq!(
            int(&cmd_bitpos(&mut s, 0, NOW, &req(&[b"BITPOS", b"k", b"0"]))),
            12
        );
        // First 1 bit is at position 0.
        assert_eq!(
            int(&cmd_bitpos(&mut s, 0, NOW, &req(&[b"BITPOS", b"k", b"1"]))),
            0
        );
        // Missing key: 0 -> 0, 1 -> -1.
        assert_eq!(
            int(&cmd_bitpos(
                &mut s,
                0,
                NOW,
                &req(&[b"BITPOS", b"missing", b"0"])
            )),
            0
        );
        assert_eq!(
            int(&cmd_bitpos(
                &mut s,
                0,
                NOW,
                &req(&[b"BITPOS", b"missing", b"1"])
            )),
            -1
        );
    }

    // ---- BITOP: AND/OR/XOR/NOT + zero-pad + empty-deletes-dest + WRONGTYPE. ----

    #[test]
    fn bitop_and_or_xor_with_zero_pad() {
        let mut s = test_store();
        store_string(&mut s, b"a", &[0b1100_1100, 0b1111_0000]);
        store_string(&mut s, b"b", &[0b1010_1010]); // shorter -> zero-padded
        // AND: byte0 = 0x88; byte1 = 0xF0 & 0x00 = 0x00. Result length = longest = 2.
        assert_eq!(
            int(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"AND", b"d", b"a", b"b"])
            )),
            2
        );
        assert_eq!(get_bytes(&mut s, b"d"), Some(vec![0b1000_1000, 0x00]));
        // The 2-byte result is a string (length-classified embstr; the module docs note
        // the divergence from Redis's always-raw). TYPE is always string.
        assert_eq!(s.type_of(0, b"d", NOW), Some(DataType::String));
        // OR.
        cmd_bitop(&mut s, 0, NOW, &req(&[b"BITOP", b"OR", b"d", b"a", b"b"]));
        assert_eq!(
            get_bytes(&mut s, b"d"),
            Some(vec![0b1110_1110, 0b1111_0000])
        );
        // XOR.
        cmd_bitop(&mut s, 0, NOW, &req(&[b"BITOP", b"XOR", b"d", b"a", b"b"]));
        assert_eq!(
            get_bytes(&mut s, b"d"),
            Some(vec![0b0110_0110, 0b1111_0000])
        );
    }

    #[test]
    fn bitop_not_inverts_single_source() {
        let mut s = test_store();
        store_string(&mut s, b"a", &[0x0F, 0xFF]);
        assert_eq!(
            int(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"NOT", b"d", b"a"])
            )),
            2
        );
        assert_eq!(get_bytes(&mut s, b"d"), Some(vec![0xF0, 0x00]));
        // NOT with more than one source is an error.
        assert_eq!(
            err_line(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"NOT", b"d", b"a", b"b"])
            )),
            "-ERR BITOP NOT must be called with a single source key."
        );
    }

    #[test]
    fn bitop_empty_result_deletes_dest() {
        let mut s = test_store();
        // Pre-existing dest that an empty result must delete.
        store_string(&mut s, b"d", b"old");
        // AND over two MISSING keys -> empty result -> dest deleted, returns 0.
        assert_eq!(
            int(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"AND", b"d", b"x", b"y"])
            )),
            0
        );
        assert_eq!(get_bytes(&mut s, b"d"), None);
        // A present + missing source: the present byte AND a zero-padded missing source is
        // not an empty result (length = the present source's length). 0x80 & 0x00 = 0x00,
        // length 1, NOT empty -> dest is written.
        cmd_setbit(&mut s, 0, NOW, &req(&[b"SETBIT", b"strk", b"0", b"1"]));
        assert_eq!(
            int(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"AND", b"d2", b"strk", b"y"])
            )),
            1
        );
        assert_eq!(get_bytes(&mut s, b"d2"), Some(vec![0x00]));
    }

    // ---- BITFIELD: multi-op array + OVERFLOW + #offset + bad-type. ----

    #[test]
    fn bitfield_multi_op_returns_array() {
        let mut s = test_store();
        // SET u8 #0 255 returns the OLD value (0); GET u8 #0 returns 255; INCRBY u8 #0 10
        // wraps to 9 (255+10 mod 256). One array of three results.
        let v = cmd_bitfield(
            &mut s,
            0,
            NOW,
            &req(&[
                b"BITFIELD",
                b"k",
                b"SET",
                b"u8",
                b"#0",
                b"255",
                b"GET",
                b"u8",
                b"#0",
                b"INCRBY",
                b"u8",
                b"#0",
                b"10",
            ]),
        );
        match v {
            Value::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(int(&items[0]), 0); // old value
                assert_eq!(int(&items[1]), 255); // get
                assert_eq!(int(&items[2]), 9); // 255 + 10 wrapped
            }
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn bitfield_overflow_modes_and_fail_nil() {
        let mut s = test_store();
        // OVERFLOW SAT then INCRBY u8 #0 300 -> saturates to 255.
        let v = cmd_bitfield(
            &mut s,
            0,
            NOW,
            &req(&[
                b"BITFIELD",
                b"k",
                b"OVERFLOW",
                b"SAT",
                b"INCRBY",
                b"u8",
                b"#0",
                b"300",
            ]),
        );
        match v {
            Value::Array(Some(items)) => assert_eq!(int(&items[0]), 255),
            other => panic!("expected an array, got {other:?}"),
        }
        // OVERFLOW FAIL then INCRBY u8 #0 1 (already 255) -> nil, value unchanged.
        let v = cmd_bitfield(
            &mut s,
            0,
            NOW,
            &req(&[
                b"BITFIELD",
                b"k",
                b"OVERFLOW",
                b"FAIL",
                b"INCRBY",
                b"u8",
                b"#0",
                b"1",
            ]),
        );
        match v {
            Value::Array(Some(items)) => assert_eq!(items[0], Value::Null),
            other => panic!("expected an array, got {other:?}"),
        }
        // The field is still 255.
        let v = cmd_bitfield(
            &mut s,
            0,
            NOW,
            &req(&[b"BITFIELD", b"k", b"GET", b"u8", b"#0"]),
        );
        match v {
            Value::Array(Some(items)) => assert_eq!(int(&items[0]), 255),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn bitfield_bad_type_error() {
        let mut s = test_store();
        assert_eq!(
            err_line(&cmd_bitfield(
                &mut s,
                0,
                NOW,
                &req(&[b"BITFIELD", b"k", b"GET", b"x8", b"0"])
            )),
            "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
        );
        // u64 specifically is rejected (i64 is allowed).
        assert_eq!(
            err_line(&cmd_bitfield(
                &mut s,
                0,
                NOW,
                &req(&[b"BITFIELD", b"k", b"GET", b"u64", b"0"])
            )),
            "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
        );
        // A bad OVERFLOW keyword.
        assert_eq!(
            err_line(&cmd_bitfield(
                &mut s,
                0,
                NOW,
                &req(&[b"BITFIELD", b"k", b"OVERFLOW", b"NOPE", b"GET", b"u8", b"0"])
            )),
            "-ERR Invalid OVERFLOW type specified"
        );
    }

    #[test]
    fn bitfield_ro_rejects_writes() {
        let mut s = test_store();
        store_string(&mut s, b"k", &[0xFF]);
        // GET works.
        let v = cmd_bitfield_ro(
            &mut s,
            0,
            NOW,
            &req(&[b"BITFIELD_RO", b"k", b"GET", b"u8", b"#0"]),
        );
        match v {
            Value::Array(Some(items)) => assert_eq!(int(&items[0]), 255),
            other => panic!("expected an array, got {other:?}"),
        }
        // SET / INCRBY / OVERFLOW are rejected.
        for op in [
            req(&[b"BITFIELD_RO", b"k", b"SET", b"u8", b"#0", b"1"]),
            req(&[b"BITFIELD_RO", b"k", b"INCRBY", b"u8", b"#0", b"1"]),
            req(&[b"BITFIELD_RO", b"k", b"OVERFLOW", b"SAT"]),
        ] {
            assert_eq!(
                err_line(&cmd_bitfield_ro(&mut s, 0, NOW, &op)),
                "-ERR BITFIELD_RO only supports the GET subcommand"
            );
        }
    }

    #[test]
    fn bitfield_signed_incrby_and_get() {
        let mut s = test_store();
        // INCRBY i8 #0 -1 on an absent key: 0 + (-1) = -1.
        let v = cmd_bitfield(
            &mut s,
            0,
            NOW,
            &req(&[b"BITFIELD", b"k", b"INCRBY", b"i8", b"#0", b"-1"]),
        );
        match v {
            Value::Array(Some(items)) => assert_eq!(int(&items[0]), -1),
            other => panic!("expected an array, got {other:?}"),
        }
        // GET i8 #0 -> -1; the backing byte is 0xFF.
        assert_eq!(get_bytes(&mut s, b"k"), Some(vec![0xFF]));
    }

    // ---- WRONGTYPE on a list/hash key + interop with GET/STRLEN/TYPE/OBJECT. ----

    #[test]
    fn bitmap_wrongtype_on_non_string() {
        let mut s = test_store();
        // Create a LIST at "lst" so the bitmap commands see a non-string type.
        let lst = req(&[b"LPUSH", b"lst", b"x"]);
        let _ = crate::cmd_list::cmd_lpush(&mut s, 0, NOW, &lst);
        let wt = "-WRONGTYPE Operation against a key holding the wrong kind of value";
        assert_eq!(
            err_line(&cmd_setbit(
                &mut s,
                0,
                NOW,
                &req(&[b"SETBIT", b"lst", b"0", b"1"])
            )),
            wt
        );
        assert_eq!(
            err_line(&cmd_getbit(
                &mut s,
                0,
                NOW,
                &req(&[b"GETBIT", b"lst", b"0"])
            )),
            wt
        );
        assert_eq!(
            err_line(&cmd_bitcount(&mut s, 0, NOW, &req(&[b"BITCOUNT", b"lst"]))),
            wt
        );
        assert_eq!(
            err_line(&cmd_bitpos(
                &mut s,
                0,
                NOW,
                &req(&[b"BITPOS", b"lst", b"1"])
            )),
            wt
        );
        assert_eq!(
            err_line(&cmd_bitfield(
                &mut s,
                0,
                NOW,
                &req(&[b"BITFIELD", b"lst", b"GET", b"u8", b"0"])
            )),
            wt
        );
        // BITOP with the list as a source is WRONGTYPE.
        assert_eq!(
            err_line(&cmd_bitop(
                &mut s,
                0,
                NOW,
                &req(&[b"BITOP", b"AND", b"d", b"lst"])
            )),
            wt
        );
    }

    #[test]
    fn setbit_value_interoperates_with_get_strlen_type() {
        let mut s = test_store();
        // Build the byte pattern of "foobar" directly, then read it through GET/TYPE.
        store_string(&mut s, b"k", b"foobar");
        // TYPE is string; the bitmap interoperates with the string reads.
        assert_eq!(s.type_of(0, b"k", NOW), Some(DataType::String));
        // GET returns the raw bytes.
        assert_eq!(get_bytes(&mut s, b"k"), Some(b"foobar".to_vec()));
        // A SETBIT on an int-encoded value treats its decimal BYTES as the bitmap and the
        // result is a string GET still reads. "123" is 3 bytes; setting bit 0 flips the
        // top bit of '1' (0x31 -> 0xB1).
        store_string(&mut s, b"n", b"123");
        assert_eq!(get_encoding(&mut s, b"n"), Some(Encoding::Int));
        cmd_setbit(&mut s, 0, NOW, &req(&[b"SETBIT", b"n", b"0", b"1"]));
        assert_eq!(get_bytes(&mut s, b"n"), Some(vec![0xB1, b'2', b'3']));
        assert_eq!(s.type_of(0, b"n", NOW), Some(DataType::String));
    }

    #[test]
    fn object_encoding_of_setbit_key_via_object_command() {
        let mut s = test_store();
        // A SETBIT-created key over the embstr threshold reports OBJECT ENCODING `raw`
        // (matching Redis's always-raw for the not-short case) and TYPE `string`. bit 400
        // -> 51 bytes, over the 44-byte threshold.
        cmd_setbit(&mut s, 0, NOW, &req(&[b"SETBIT", b"big", b"400", b"1"]));
        let enc = crate::cmd_introspect::cmd_object(
            &mut s,
            0,
            NOW,
            &req(&[b"OBJECT", b"ENCODING", b"big"]),
        );
        match enc {
            Value::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"raw"),
            other => panic!("expected a bulk encoding name, got {other:?}"),
        }
    }

    /// Store a raw string value at `key` via the store's blind upsert (a test helper so
    /// the bitmap tests can seed arbitrary byte patterns without going through SETBIT).
    fn store_string(store: &mut TestStore, key: &[u8], bytes: &[u8]) {
        use ironcache_storage::{ExpireWrite, NewValue};
        store.upsert(0, key, NewValue::Bytes(bytes), ExpireWrite::Clear, NOW);
    }
}
