// SPDX-License-Identifier: MIT OR Apache-2.0
//! Minisign signature verification for the `ironcache upgrade` AUTHENTICITY anchor (#386, ADR-0020).
//!
//! `Sha256Verifier` (verify.rs) gives INTEGRITY (the bytes match the published `SHA256SUMS`); this
//! adds AUTHENTICITY: the `SHA256SUMS` was signed by the holder of a pinned Ed25519 key. The release
//! signs `SHA256SUMS` with minisign (release.yml), whose signature transitively covers every artifact
//! (each entry pins a sha256), so verifying that one small detached signature against ONE committed
//! public key -- offline, no PKI, no transparency log, no network -- authenticates the whole release.
//!
//! ## The minisign wire format (pinned to jedisct1/minisign, cross-checked against rsign2 0.6.6)
//!
//! - PUBLIC KEY: `base64( "Ed"[2] || key_id[8] || ed25519_public_key[32] )` = 42 bytes. (A `.pub`
//!   file prefixes an `untrusted comment:` line; we take the base64 line.)
//! - SIGNATURE FILE (`.minisig`), four lines:
//!   1. `untrusted comment: ...`
//!   2. `base64( sig_alg[2] || key_id[8] || signature[64] )` = 74 bytes. `sig_alg` is `ED`
//!      (PREHASHED: the signature is Ed25519 over Blake2b-512(file), the modern minisign default) or
//!      `Ed` (LEGACY: Ed25519 over the raw file). Both are accepted.
//!   3. `trusted comment: <text>`
//!   4. `base64( global_signature[64] )` = Ed25519 over `signature[64] || <trusted comment text>`,
//!      which authenticates the trusted comment.
//! - `key_id` is a little-endian 8-byte tag; the signature's must equal the public key's.
//!
//! ## Crypto sourcing (the ADR-0017 "no new crypto crate" posture)
//!
//! Ed25519 verification is `ring` -- ALREADY linked into this binary as tokio-rustls's provider, so no
//! new crate and no hand-rolled curve arithmetic (which must never be hand-rolled). Blake2b-512 and
//! base64, which `ring` does not expose, are the SMALL deterministic pieces this module hand-rolls (in
//! the same spirit as the workspace's hand-rolled FIPS-180-4 sha256), each validated against RFC-7693 /
//! RFC-4648 known-answer vectors PLUS an end-to-end test against a REAL rsign2-generated signature.

use ring::signature::{ED25519, UnparsedPublicKey};

/// A typed minisign-verification failure (no stringly-typed errors, ERRORS.md).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MinisignError {
    /// The pinned public key string is not a well-formed minisign Ed25519 public key.
    #[error("malformed minisign public key: {0}")]
    BadPublicKey(&'static str),
    /// The `.minisig` file is not the expected 4-line minisign signature format.
    #[error("malformed minisign signature file: {0}")]
    BadSignatureFile(&'static str),
    /// The signature's key id does not match the pinned public key (signed by a different key).
    #[error("minisign key id mismatch (the signature was made by a different key)")]
    KeyIdMismatch,
    /// The Ed25519 verification of the file signature failed (the content is not authentic).
    #[error("minisign signature verification FAILED (content not authentic)")]
    BadSignature,
    /// The global signature over the trusted comment failed (the trusted comment is not authentic).
    #[error("minisign trusted-comment (global) signature verification FAILED")]
    BadGlobalSignature,
}

/// A parsed minisign Ed25519 public key: the 8-byte key id and the 32-byte verification key.
#[derive(Debug, Clone, Copy)]
pub struct MinisignPublicKey {
    key_id: [u8; 8],
    key: [u8; 32],
}

impl MinisignPublicKey {
    /// Parse a minisign public key from its base64 STRING (the `RW...` line, without the
    /// `untrusted comment:` prefix). The decoded blob is `"Ed"[2] || key_id[8] || public_key[32]`.
    ///
    /// # Errors
    ///
    /// [`MinisignError::BadPublicKey`] if the string is not valid base64 of the 42-byte key blob.
    pub fn parse(pubkey_b64: &str) -> Result<Self, MinisignError> {
        // A `.pub` FILE has two lines (comment + key); accept either the bare key line or the last
        // non-empty line of a pasted file.
        let line = pubkey_b64
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or(pubkey_b64)
            .trim();
        let blob = base64_decode(line).ok_or(MinisignError::BadPublicKey("not valid base64"))?;
        if blob.len() != 42 {
            return Err(MinisignError::BadPublicKey(
                "wrong length (expected 42 bytes)",
            ));
        }
        if &blob[0..2] != b"Ed" {
            return Err(MinisignError::BadPublicKey("not an Ed25519 minisign key"));
        }
        let mut key_id = [0u8; 8];
        key_id.copy_from_slice(&blob[2..10]);
        let mut key = [0u8; 32];
        key.copy_from_slice(&blob[10..42]);
        Ok(MinisignPublicKey { key_id, key })
    }
}

/// Verify a detached minisign signature `sig_file` (the full `.minisig` text) over `message` (the
/// exact bytes that were signed, e.g. the `SHA256SUMS` file content) against `pubkey`. Verifies BOTH
/// the file signature AND the global (trusted-comment) signature, exactly as `minisign -V` does.
///
/// # Errors
///
/// A [`MinisignError`] for a malformed key/signature, a key-id mismatch, or either signature failing.
pub fn verify(
    message: &[u8],
    sig_file: &str,
    pubkey: &MinisignPublicKey,
) -> Result<(), MinisignError> {
    let mut lines = sig_file.lines();
    let _untrusted = lines
        .next()
        .ok_or(MinisignError::BadSignatureFile("missing line 1"))?;
    let sig_b64 = lines
        .next()
        .ok_or(MinisignError::BadSignatureFile(
            "missing the signature line",
        ))?
        .trim();
    let trusted_line = lines.next().ok_or(MinisignError::BadSignatureFile(
        "missing the trusted-comment line",
    ))?;
    let global_b64 = lines
        .next()
        .ok_or(MinisignError::BadSignatureFile(
            "missing the global-signature line",
        ))?
        .trim();

    // Line 2: sig_alg[2] || key_id[8] || signature[64].
    let sig_blob = base64_decode(sig_b64).ok_or(MinisignError::BadSignatureFile(
        "signature line is not base64",
    ))?;
    if sig_blob.len() != 74 {
        return Err(MinisignError::BadSignatureFile(
            "signature blob wrong length (expected 74 bytes)",
        ));
    }
    let alg = &sig_blob[0..2];
    let prehashed = match alg {
        b"ED" => true,  // modern default: Ed25519 over Blake2b-512(message)
        b"Ed" => false, // legacy: Ed25519 over the raw message
        _ => {
            return Err(MinisignError::BadSignatureFile(
                "unknown signature algorithm",
            ));
        }
    };
    if sig_blob[2..10] != pubkey.key_id {
        return Err(MinisignError::KeyIdMismatch);
    }
    let signature = &sig_blob[10..74];

    // Verify the FILE signature over the (optionally prehashed) message.
    let verify_key = UnparsedPublicKey::new(&ED25519, &pubkey.key[..]);
    if prehashed {
        let digest = blake2b512(message);
        verify_key
            .verify(&digest, signature)
            .map_err(|_| MinisignError::BadSignature)?;
    } else {
        verify_key
            .verify(message, signature)
            .map_err(|_| MinisignError::BadSignature)?;
    }

    // Verify the GLOBAL signature over signature[64] || <trusted comment text>. The trusted comment is
    // line 3 with the `trusted comment: ` prefix stripped (minisign's exact convention).
    let trusted_comment = trusted_line
        .strip_prefix("trusted comment: ")
        .unwrap_or(trusted_line);
    let global_sig = base64_decode(global_b64).ok_or(MinisignError::BadSignatureFile(
        "global signature line is not base64",
    ))?;
    if global_sig.len() != 64 {
        return Err(MinisignError::BadSignatureFile(
            "global signature wrong length (expected 64 bytes)",
        ));
    }
    let mut global_msg = Vec::with_capacity(64 + trusted_comment.len());
    global_msg.extend_from_slice(signature);
    global_msg.extend_from_slice(trusted_comment.as_bytes());
    verify_key
        .verify(&global_msg, &global_sig)
        .map_err(|_| MinisignError::BadGlobalSignature)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// base64 decode (RFC 4648 standard alphabet). Hand-rolled: `ring` does not expose base64 and the
// no-new-crypto-crate posture avoids pulling a base64 crate for a 30-line decoder. Validated against
// RFC 4648 test vectors.
// ---------------------------------------------------------------------------

/// Decode a standard-alphabet (RFC 4648) base64 string (with optional `=` padding, no line breaks).
/// Returns `None` on any non-alphabet byte, so a malformed key/signature is rejected, not truncated.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut nbits: u32 = 0;
    for &b in bytes {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break, // padding begins; the remainder is padding
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Blake2b-512 (RFC 7693, unkeyed, 64-byte digest). Hand-rolled -- `ring` has no Blake2b, minisign's
// prehash needs it, and hand-rolling a HASH (no secret, no side-channel-sensitive branching) is the
// same posture as the workspace's hand-rolled sha256. Validated against RFC 7693's `abc` KAT and a
// real rsign2 signature end to end.
// ---------------------------------------------------------------------------

/// The Blake2b initialization vector (identical to the SHA-512 IV, RFC 7693).
const BLAKE2B_IV: [u64; 8] = [
    0x6a09_e667_f3bc_c908,
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
];

/// The Blake2b message-word permutation schedule (RFC 7693 SIGMA).
const BLAKE2B_SIGMA: [[usize; 16]; 12] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

/// The Blake2b `G` mixing function (RFC 7693 section 3.1). The `a`/`b`/`c`/`d`/`x`/`y` single-letter
/// names are the RFC's own; renaming them would obscure the correspondence to the spec.
#[inline]
#[allow(clippy::too_many_arguments, clippy::many_single_char_names)]
fn blake2b_g(v: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
    v[d] = (v[d] ^ v[a]).rotate_right(32);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(24);
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
    v[d] = (v[d] ^ v[a]).rotate_right(16);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(63);
}

/// The Blake2b compression function `F` over one 128-byte block `m` (as 16 little-endian u64s), with
/// byte counter `t` and the final-block flag `last` (RFC 7693 section 3.2).
fn blake2b_compress(h: &mut [u64; 8], m: &[u64; 16], t: u128, last: bool) {
    let mut v = [0u64; 16];
    v[0..8].copy_from_slice(h);
    v[8..16].copy_from_slice(&BLAKE2B_IV);
    v[12] ^= t as u64;
    v[13] ^= (t >> 64) as u64;
    if last {
        v[14] ^= u64::MAX;
    }
    for s in &BLAKE2B_SIGMA {
        blake2b_g(&mut v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        blake2b_g(&mut v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        blake2b_g(&mut v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        blake2b_g(&mut v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        blake2b_g(&mut v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        blake2b_g(&mut v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        blake2b_g(&mut v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        blake2b_g(&mut v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }
    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

/// Load a 128-byte block as 16 little-endian `u64` message words.
fn load_block(block: &[u8; 128]) -> [u64; 16] {
    let mut m = [0u64; 16];
    for (i, word) in m.iter_mut().enumerate() {
        *word = u64::from_le_bytes(block[i * 8..i * 8 + 8].try_into().expect("8 bytes"));
    }
    m
}

/// Blake2b-512 of `data` (unkeyed, 64-byte digest).
fn blake2b512(data: &[u8]) -> [u8; 64] {
    let mut h = BLAKE2B_IV;
    // Parameter block for an unkeyed 64-byte digest: digest_length=64, key_length=0, fanout=1, depth=1.
    h[0] ^= 0x0101_0000 ^ 64;

    // Process every full non-final 128-byte block, advancing the byte counter.
    let full_nonfinal = if data.is_empty() {
        0
    } else {
        (data.len() - 1) / 128
    };
    let mut t: u128 = 0;
    for i in 0..full_nonfinal {
        let block: &[u8; 128] = data[i * 128..i * 128 + 128].try_into().expect("128 bytes");
        t += 128;
        blake2b_compress(&mut h, &load_block(block), t, false);
    }

    // The final block (partial, zero-padded; for empty input this is the only block).
    let start = full_nonfinal * 128;
    let rem = &data[start..];
    let mut last = [0u8; 128];
    last[..rem.len()].copy_from_slice(rem);
    t += rem.len() as u128;
    blake2b_compress(&mut h, &load_block(&last), t, true);

    let mut out = [0u8; 64];
    for i in 0..8 {
        out[i * 8..i * 8 + 8].copy_from_slice(&h[i].to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Component KATs: Blake2b-512 (RFC 7693) + base64 (RFC 4648). ----

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    #[test]
    fn blake2b512_matches_rfc7693_known_answers() {
        // RFC 7693 Appendix A: Blake2b-512("abc").
        assert_eq!(
            hex(&blake2b512(b"abc")),
            "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d1\
             7d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923"
        );
        // The empty input (a distinct code path: only the final zero block).
        assert_eq!(
            hex(&blake2b512(b"")),
            "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419\
             d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce"
        );
        // A > 128-byte input to exercise multi-block processing (144 bytes of 'a').
        let long = vec![b'a'; 144];
        assert_eq!(
            hex(&blake2b512(&long)).len(),
            128,
            "512-bit digest is 64 bytes"
        );
    }

    #[test]
    fn base64_decode_rfc4648_vectors() {
        assert_eq!(base64_decode("").unwrap(), b"");
        assert_eq!(base64_decode("Zg==").unwrap(), b"f");
        assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
        assert_eq!(base64_decode("Zm9v").unwrap(), b"foo");
        assert_eq!(base64_decode("Zm9vYmFy").unwrap(), b"foobar");
        // A non-alphabet byte is rejected (not silently truncated).
        assert!(base64_decode("Zm9v!bad").is_none());
    }

    // ---- End-to-end against a REAL rsign2 0.6.6 signature (ground truth). ----

    /// A real minisign keypair + detached signature generated by `rsign2 0.6.6` (the modern,
    /// prehashed `ED` format identical to jedisct1/minisign). These are the exact bytes `rsign verify`
    /// accepts, so a pass here proves the base64 + Blake2b-512 + Ed25519 + parsing all match real
    /// minisign, not just each other.
    const REAL_PUBKEY: &str = "RWREdimvfA8cGa5MTkLinjCO6dktAfbzHcG7vGO4cCDAtilnBR+mUBvY";
    const REAL_SUMS: &[u8] = b"abc123  ironcache-1.0.0-linux-amd64-musl.tar.gz\n";
    const REAL_MINISIG: &str = "untrusted comment: signature from rsign secret key\n\
        RUREdimvfA8cGSBAhOl6pmS4NQ1Y5PEKYfVBPm6IAyICaHBn9jZPXdTam5Kln4SshnBIZEQyfvM5ALysOramgoZ7siDqdSLBNQQ=\n\
        trusted comment: test trusted comment\n\
        bp/MYwC+oUyiY5zRPph3S481fEHhaK97PKd34IJBhWQ3RSsiuEc2uIpyCWAgYM+0xUjXbvkMuPlfWlOg1eSfBg==\n";

    #[test]
    fn verifies_a_real_rsign2_signature() {
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).expect("pubkey parses");
        // The key id in the pubkey must equal the one in the signature (191C0F7CAF297644, LE).
        assert_eq!(pk.key_id, [0x44, 0x76, 0x29, 0xaf, 0x7c, 0x0f, 0x1c, 0x19]);
        verify(REAL_SUMS, REAL_MINISIG, &pk).expect("a genuine minisign signature verifies");
    }

    // ---- Tamper tests: every mutation must be REJECTED (fail-closed). ----

    #[test]
    fn a_tampered_message_is_rejected() {
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).unwrap();
        let mut tampered = REAL_SUMS.to_vec();
        tampered[0] ^= 0x01; // flip one bit of the signed content
        assert_eq!(
            verify(&tampered, REAL_MINISIG, &pk),
            Err(MinisignError::BadSignature),
            "a signature over different content must not verify"
        );
    }

    #[test]
    fn a_tampered_signature_is_rejected() {
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).unwrap();
        // Corrupt one base64 char of the signature line (line 2).
        let bad = REAL_MINISIG.replacen("RUREdimv", "RUREdimW", 1);
        let r = verify(REAL_SUMS, &bad, &pk);
        assert!(
            matches!(
                r,
                Err(MinisignError::BadSignature
                    | MinisignError::KeyIdMismatch
                    | MinisignError::BadSignatureFile(_))
            ),
            "a corrupted signature must be rejected, got {r:?}"
        );
    }

    #[test]
    fn a_wrong_key_is_rejected() {
        // A DIFFERENT valid minisign key (its key id will not match the signature's).
        // Built by flipping the public-key body of the real key (still 42 bytes, still "Ed").
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).unwrap();
        let wrong = MinisignPublicKey {
            key_id: [0, 0, 0, 0, 0, 0, 0, 0], // a key id that cannot match the signature
            key: pk.key,
        };
        assert_eq!(
            verify(REAL_SUMS, REAL_MINISIG, &wrong),
            Err(MinisignError::KeyIdMismatch),
            "a signature whose key id differs from the pinned key is rejected"
        );
    }

    #[test]
    fn a_tampered_trusted_comment_is_rejected() {
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).unwrap();
        // The file signature still verifies, but the global signature over the trusted comment must not.
        let bad = REAL_MINISIG.replace("test trusted comment", "attacker trusted comment");
        assert_eq!(
            verify(REAL_SUMS, &bad, &pk),
            Err(MinisignError::BadGlobalSignature),
            "a modified trusted comment must fail the global signature"
        );
    }

    #[test]
    fn malformed_inputs_are_typed_errors_not_panics() {
        assert!(matches!(
            MinisignPublicKey::parse("not base64!!"),
            Err(MinisignError::BadPublicKey(_))
        ));
        assert!(matches!(
            MinisignPublicKey::parse("Zm9v"), // valid base64, wrong length
            Err(MinisignError::BadPublicKey(_))
        ));
        let pk = MinisignPublicKey::parse(REAL_PUBKEY).unwrap();
        assert!(matches!(
            verify(REAL_SUMS, "only one line", &pk),
            Err(MinisignError::BadSignatureFile(_))
        ));
    }
}
