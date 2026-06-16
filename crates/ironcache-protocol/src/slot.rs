// SPDX-License-Identifier: MIT OR Apache-2.0
//! The Redis-Cluster slot space: CRC16/XMODEM hashing, hash-tag co-location, and the
//! 16384-slot key mapping (CLUSTER_CONTRACT.md #70).
//!
//! This module is the PURE, client-visible slot layer adopted verbatim from Redis
//! Cluster so an unmodified client routes against IronCache exactly as it would against
//! a real cluster: a key's slot is `CRC16(hash_tag(key)) % 16384` using the XMODEM CRC16
//! variant (poly 0x1021, init 0x0000, no reflection, no output XOR)
//! [redis-cluster-crc16-xmodem], over the 16384-slot space [redis-cluster-16384-slots].
//!
//! It is INDEPENDENT of the internal FNV-1a shard hash (`route::hash64`): this is the
//! WIRE contract a client observes, not how IronCache partitions data internally
//! (ADR-0025). In slice 1 (cluster-disabled) nothing routes by these functions; they
//! back the read-only `CLUSTER KEYSLOT` introspection and are the foundation later
//! slices build slot routing on.
//!
//! The crate is `no`-OS-time and `no`-rand by construction (a pure codec); these
//! functions are pure deterministic functions of their byte input, so they satisfy the
//! determinism invariant trivially. A clear bitwise CRC16 is used (not a table): this is
//! NOT a hot path in slice 1, and the bitwise form is obviously correct against the
//! XMODEM definition.

/// The number of hash slots in the Redis-Cluster wire space (16384), adopted verbatim
/// [redis-cluster-16384-slots]. A key's slot is its CRC16 reduced into this range.
pub const CLUSTER_SLOTS: u16 = 16384;

/// CRC16/XMODEM of `data`: polynomial 0x1021, initial value 0x0000, NO input reflection,
/// NO output reflection, NO output XOR. This is the exact variant Redis Cluster uses to
/// map a key to a slot [redis-cluster-crc16-xmodem], so the result agrees bit-for-bit
/// with a reference cluster.
///
/// A clear bitwise implementation (8 shifts per byte) is used in preference to a lookup
/// table: it is obviously correct against the XMODEM definition and slot hashing is not a
/// hot path in slice 1 (it backs only the read-only `CLUSTER KEYSLOT` introspection).
///
/// The canonical check value `CRC16("123456789") == 0x31C3` is asserted in the unit
/// tests below.
#[must_use]
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x0000;
    for &byte in data {
        // XMODEM is a non-reflected MSB-first CRC: the incoming byte is aligned to the
        // top of the 16-bit register, then 8 polynomial-division steps are applied.
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// The substring of `key` that Redis Cluster hashes for slot assignment, applying the
/// hash-tag rule [redis-cluster-hash-tags][redis-cluster-hash-tag-rule]: only the bytes
/// between the FIRST `{` and the FIRST `}` AFTER it are returned, and ONLY when that
/// substring is non-empty; otherwise the WHOLE key is returned.
///
/// This means `{user1000}.following` and `{user1000}.followers` both hash by `user1000`
/// (so they co-locate), while `foo{}` (empty braces), `{}{tag}` (the first brace pair is
/// empty), and `{tag` (no closing brace) all hash by the whole key. Exactly Redis's
/// `keyHashSlot` tag-extraction (src/cluster.c).
#[must_use]
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(rel) = key[open + 1..].iter().position(|&b| b == b'}') {
            if rel != 0 {
                return &key[open + 1..open + 1 + rel];
            }
        }
    }
    key
}

/// The cluster slot a `key` maps to: `CRC16(hash_tag(key)) % 16384`. This is the
/// client-visible routing function Redis Cluster uses [redis-cluster-crc16-xmodem]; the
/// hash-tag rule ([`hash_tag`]) co-locates tagged keys onto one slot.
#[must_use]
pub fn key_slot(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % CLUSTER_SLOTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_xmodem_check_value() {
        // The canonical CRC16/XMODEM check value: CRC16("123456789") == 0x31C3. This is
        // the standard test vector for the variant and pins poly/init/no-reflection.
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn crc16_is_deterministic_and_pure() {
        // A pure function: the same input always yields the same output, and the empty
        // input is the init value (0x0000) since no bytes are processed.
        assert_eq!(crc16(b""), 0x0000);
        assert_eq!(crc16(b"foo"), crc16(b"foo"));
        assert_ne!(crc16(b"foo"), crc16(b"bar"));
    }

    #[test]
    fn key_slot_reference_vectors() {
        // Independently verified against a reference Redis Cluster (CRC16/XMODEM mod
        // 16384). These pin the whole pipeline (crc16 + hash_tag + modulo).
        assert_eq!(key_slot(b"123456789"), 12739);
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        // The empty key hashes the empty string -> CRC16 init 0x0000 -> slot 0.
        assert_eq!(key_slot(b""), 0);
    }

    #[test]
    fn hash_tag_co_locates_tagged_keys() {
        // The hash-tag rule: `{user1000}.following` and `{user1000}.followers` both hash
        // by `user1000`, so they share a slot; the bare `user1000` hashes the same bytes,
        // so all three co-locate.
        assert_eq!(key_slot(b"{user1000}.following"), 3443);
        assert_eq!(key_slot(b"{user1000}.followers"), 3443);
        assert_eq!(key_slot(b"user1000"), 3443);
        assert_eq!(
            key_slot(b"{user1000}.following"),
            key_slot(b"{user1000}.followers")
        );
    }

    #[test]
    fn hash_tag_fallbacks_use_whole_key() {
        // Empty braces `{}` -> the substring is empty, so the WHOLE key is hashed.
        assert_eq!(key_slot(b"foo{}"), 5542);
        // The FIRST brace pair `{}` is empty -> the whole key is hashed (Redis only looks
        // at the first `{` and the first `}` after it; it does NOT scan for a later
        // non-empty pair).
        assert_eq!(key_slot(b"{}{tag}"), 11440);
        // A non-empty first brace pair IS used: `foo{bar}{zap}` hashes by `bar`, the same
        // bytes as the bare key `bar`.
        assert_eq!(key_slot(b"foo{bar}{zap}"), 5061);
        assert_eq!(key_slot(b"foo{bar}{zap}"), key_slot(b"bar"));
    }

    #[test]
    fn hash_tag_extraction_edge_cases() {
        // No closing brace -> the whole key.
        assert_eq!(hash_tag(b"{tag"), b"{tag");
        // No brace at all -> the whole key.
        assert_eq!(hash_tag(b"foo"), b"foo");
        // A well-formed non-empty tag -> just the tag bytes.
        assert_eq!(hash_tag(b"{user1000}.following"), b"user1000");
        // Empty braces -> the whole key (the substring between `{` and `}` is empty).
        assert_eq!(hash_tag(b"foo{}"), b"foo{}");
        // The first brace pair is empty -> the whole key.
        assert_eq!(hash_tag(b"{}{tag}"), b"{}{tag}");
    }

    #[test]
    fn key_slot_is_always_in_range_for_a_corpus() {
        // PROPERTY: every key maps into [0, 16384). A representative corpus (tagged,
        // empty-brace, binary, and long keys) all land in range.
        let corpus: &[&[u8]] = &[
            b"",
            b"a",
            b"foo",
            b"bar",
            b"123456789",
            b"{user1000}.following",
            b"{}{tag}",
            b"foo{bar}{zap}",
            b"\x00\xff\x01\xfe",
            b"a-very-long-key-name-that-exceeds-a-handful-of-bytes-for-coverage",
        ];
        for key in corpus {
            assert!(
                key_slot(key) < CLUSTER_SLOTS,
                "slot for {:?} out of range",
                String::from_utf8_lossy(key)
            );
        }
    }
}
