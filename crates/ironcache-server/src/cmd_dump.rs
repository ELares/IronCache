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
    DUMP_RDB_VERSION, RDB_TYPE_STRING, RestoreParseError, crc64, read_rdb_string, verify_footer,
    write_rdb_len,
};

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
}
