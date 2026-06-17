// SPDX-License-Identifier: MIT OR Apache-2.0
//! The replication cursor: the `(replid, offset)` pair that names a position in a
//! primary's replication stream (HA-7a).
//!
//! A replica resumes from a primary by telling it "I am caught up through OFFSET of
//! stream REPLID". The pair is the whole resume contract:
//!
//! - [`ReplId`] identifies WHICH stream. It is the primary's replication id, a
//!   20-byte value rendered on the wire as 40 lowercase hex chars (the Redis
//!   `replid` convention). A new primary (or a primary that has lost its history)
//!   advertises a DIFFERENT replid, which is how a replica learns it must do a full
//!   re-sync rather than resume; in 7a the replid is a fixed, externally supplied
//!   value (HA-7d derives/rotates it on role change).
//! - [`ReplOffset`] identifies HOW FAR. It is a MONOTONIC LOGICAL write-sequence
//!   number, NOT a byte offset: it counts replicated WRITES, incrementing once per
//!   write. A logical offset is sufficient for the snapshot+stream protocol and far
//!   simpler than byte accounting. In HA-7c the primary advances it from the HA-5a
//!   write-observation seam (one tick per observed write); in 7a, where no data is
//!   on the wire yet, the primary advances it trivially (e.g. per heartbeat tick)
//!   so the cursor mechanism is exercised end to end. It never goes backwards.

/// A primary's replication id: 20 raw bytes, rendered as 40 lowercase hex on the
/// wire (the Redis `replid` convention).
///
/// Two replicas resuming against the SAME `ReplId` are on the same stream and may
/// resume from their offsets; a CHANGED `ReplId` means the stream identity changed
/// (a new primary, or history loss) and the replica must full-sync (HA-7b). It is a
/// `Copy` value newtype so it threads cheaply through the link state machine and the
/// [`crate::frames::Frame::ReplPing`] heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplId([u8; 20]);

impl ReplId {
    /// The number of raw bytes in a replication id.
    pub const LEN: usize = 20;
    /// The number of hex characters in the wire form (`2 * LEN`).
    pub const HEX_LEN: usize = 40;

    /// Construct from the 20 raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        ReplId(bytes)
    }

    /// The raw 20 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Parse a 40-char lowercase/uppercase hex wire form into a [`ReplId`].
    ///
    /// Returns `None` unless `hex` is exactly [`ReplId::HEX_LEN`] ASCII hex digits;
    /// the [`crate::frames`] decoder maps that `None` to a framing error so a
    /// corrupt replid never becomes a fabricated stream identity.
    #[must_use]
    pub fn from_hex(hex: &[u8]) -> Option<Self> {
        if hex.len() != Self::HEX_LEN {
            return None;
        }
        let mut out = [0u8; Self::LEN];
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = hex_val(hex[i * 2])?;
            let lo = hex_val(hex[i * 2 + 1])?;
            *byte = (hi << 4) | lo;
        }
        Some(ReplId(out))
    }

    /// The 40-char lowercase hex wire form.
    #[must_use]
    pub fn as_hex(&self) -> String {
        let mut s = String::with_capacity(Self::HEX_LEN);
        for b in self.0 {
            s.push(hex_digit(b >> 4));
            s.push(hex_digit(b & 0x0f));
        }
        s
    }
}

/// The hex digit for a nibble in `[0, 16)`.
fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + (nibble - 10)) as char,
    }
}

/// The nibble value of an ASCII hex digit, or `None` if it is not one.
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// A monotonic LOGICAL write-sequence offset into a replication stream.
///
/// Not a byte offset: it counts replicated WRITES (one increment per write),
/// advanced per write in HA-7c from the HA-5a observation seam. It is the "how far"
/// half of the [resume cursor](self); a replica's last-acked `ReplOffset` is the
/// point it asks the primary to resume from after a reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ReplOffset(pub u64);

impl ReplOffset {
    /// The stream origin (no writes replicated yet); a fresh replica's ack.
    pub const ZERO: ReplOffset = ReplOffset(0);

    /// The next offset (`self + 1`), saturating at the representable maximum.
    ///
    /// One logical write advances the offset by one. Saturating keeps the cursor
    /// panic-free at the (practically unreachable) `u64::MAX` ceiling; the offset
    /// never decreases.
    #[must_use]
    pub fn next(self) -> ReplOffset {
        ReplOffset(self.0.saturating_add(1))
    }

    /// The larger of `self` and `other`. Used to advance an observed/acked offset
    /// monotonically: a smaller value (a stale or reordered frame) never moves the
    /// cursor backwards.
    #[must_use]
    pub fn max_with(self, other: ReplOffset) -> ReplOffset {
        ReplOffset(self.0.max(other.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replid_hex_round_trips() {
        let raw = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0xff, 0x10, 0x20, 0x30, 0x40,
            0x50, 0x60, 0x70, 0x80, 0x90, 0xa0,
        ];
        let id = ReplId::from_bytes(raw);
        let hex = id.as_hex();
        assert_eq!(hex.len(), ReplId::HEX_LEN);
        assert_eq!(hex, "0123456789abcdef00ff102030405060708090a0");
        // Parse the rendered hex back to the same id (lowercase) ...
        assert_eq!(ReplId::from_hex(hex.as_bytes()), Some(id));
        // ... and an uppercase spelling decodes to the same bytes.
        assert_eq!(ReplId::from_hex(hex.to_uppercase().as_bytes()), Some(id));
    }

    #[test]
    fn replid_from_hex_rejects_bad_input() {
        // Too short / too long.
        assert_eq!(ReplId::from_hex(b"abc"), None);
        assert_eq!(ReplId::from_hex(&[b'a'; 41]), None);
        // Right length, non-hex char.
        let mut forty = [b'a'; 40];
        forty[10] = b'z';
        assert_eq!(ReplId::from_hex(&forty), None);
    }

    #[test]
    fn offset_advances_monotonically() {
        assert_eq!(ReplOffset::ZERO, ReplOffset(0));
        assert_eq!(ReplOffset(5).next(), ReplOffset(6));
        // Saturates at the ceiling rather than wrapping.
        assert_eq!(ReplOffset(u64::MAX).next(), ReplOffset(u64::MAX));
        // max_with never moves backwards.
        assert_eq!(ReplOffset(7).max_with(ReplOffset(3)), ReplOffset(7));
        assert_eq!(ReplOffset(3).max_with(ReplOffset(7)), ReplOffset(7));
    }
}
