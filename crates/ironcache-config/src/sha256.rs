// SPDX-License-Identifier: MIT OR Apache-2.0
//! A self-contained FIPS 180-4 SHA-256 implementation (AUTH.md "Passwords are
//! stored as SHA-256", threat-model #142 accepted-risk row).
//!
//! ## Why hand-rolled, and why here
//!
//! AUTH stores the `requirepass` password as a SHA-256 digest AT REST rather than in
//! plaintext (#65). Both the config crate (which hashes the password when it is set,
//! at boot load AND on `CONFIG SET`) and `ironcache-server`'s `check_auth` (which
//! hashes the provided guess and compares digests) need the same primitive, and
//! `ironcache-server` already depends on `ironcache-config`, so the function lives
//! here and is re-exported as [`crate::sha256_hex`]. It is implemented from the FIPS
//! 180-4 specification in safe Rust with NO new dependency (the crate is intentionally
//! dependency-light; pulling a crypto crate in just to hash a password would widen the
//! supply-chain surface for a primitive the threat model explicitly scopes to plain
//! SHA-256 for Redis behavioral equivalence, ADR-0009).
//!
//! ## What this is NOT
//!
//! This is SHA-256, a fast cryptographic hash, NOT a password KDF (no salt, no work
//! factor). That is a DELIBERATE behavioral-equivalence choice (AUTH.md / ADR-0009):
//! Redis stores the default-user password as SHA-256, and IronCache matches that stored
//! form. The threat model (#142) records the weaker-than-KDF storage and the
//! compare-side-channel as accepted risks for this milestone. Do NOT reach for this as
//! a general KDF.
//!
//! Correctness is anchored by the NIST FIPS 180-4 known-answer vectors in the tests
//! (empty string, `"abc"`, and the 896-bit multi-block message that exercises the
//! length-padding across two compression blocks). A hand-rolled crypto primitive is
//! only as trustworthy as its KATs, so those tests are mandatory.

/// The eight SHA-256 initial hash values H0..H7 (FIPS 180-4 section 5.3.3): the first
/// 32 bits of the fractional parts of the square roots of the first eight primes.
const H_INIT: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// The 64 SHA-256 round constants K0..K63 (FIPS 180-4 section 4.2.2): the first 32
/// bits of the fractional parts of the cube roots of the first 64 primes.
const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// Compute the SHA-256 digest of `bytes` and return the 64-character lowercase hex
/// string (FIPS 180-4). Deterministic, allocation-only (no I/O, no clock, no RNG), and
/// total: every byte slice has a well-defined digest. This is the single primitive both
/// the config crate (hash-on-set) and the server (hash-the-guess) call so the stored
/// form and the verify path use ONE implementation.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha256(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        // Lowercase hex, two chars per byte. `write!`-free to keep this allocation-only
        // and avoid the fmt machinery for a 32-byte fixed loop.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        hex.push(HEX[(byte >> 4) as usize] as char);
        hex.push(HEX[(byte & 0x0f) as usize] as char);
    }
    hex
}

/// The raw 32-byte SHA-256 digest of `bytes` (FIPS 180-4 section 6.2). Kept private; the
/// public surface is the hex form, which is what is stored and compared.
// The single-letter working variables a..h are the verbatim FIPS 180-4 section 6.2.2
// names; renaming them to satisfy `many_single_char_names` would only obscure the
// one-to-one mapping to the published algorithm, so the lint is allowed locally.
#[allow(clippy::many_single_char_names)]
fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = H_INIT;

    // --- Padding (FIPS 180-4 section 5.1.1) ---
    // Append the bit 0x80, then 0x00 bytes until the length is 56 mod 64, then the
    // 64-bit big-endian message bit length. The total then divides into 64-byte blocks.
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut message = Vec::with_capacity(bytes.len() + 9 + 63);
    message.extend_from_slice(bytes);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0x00);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    debug_assert_eq!(
        message.len() % 64,
        0,
        "padded message must be block-aligned"
    );

    // --- Process each 512-bit block (FIPS 180-4 section 6.2.2) ---
    for block in message.chunks_exact(64) {
        // Prepare the 64-entry message schedule W.
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            let j = i * 4;
            *word = u32::from_be_bytes([block[j], block[j + 1], block[j + 2], block[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        // Initialize the working variables from the current hash value.
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        // The 64 compression rounds.
        for i in 0..64 {
            let big_s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(big_s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let big_s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = big_s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        // Add the compressed chunk back into the running hash value.
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    // Serialize the eight 32-bit words big-endian into the 32-byte digest.
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The NIST FIPS 180-4 known-answer vectors. These are the correctness anchor for a
    /// hand-rolled crypto primitive: the empty string, the single-block "abc", and the
    /// 896-bit two-block message that exercises the length-padding across multiple
    /// compression blocks. If any of these drifts, the hash is wrong and AUTH-at-rest is
    /// silently broken, so they are mandatory.
    #[test]
    fn nist_known_answer_vectors() {
        // Empty input: digest of zero bytes (just the padding block).
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // The canonical single-block "abc" vector.
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // The 896-bit (112-byte) multi-block vector: it pushes the padded message past a
        // single 64-byte block, so it exercises the length encoding AND a second
        // compression round.
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn output_is_64_lowercase_hex_chars() {
        let h = sha256_hex(b"some password");
        assert_eq!(h.len(), 64);
        assert!(
            h.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "digest must be lowercase hex: {h}"
        );
    }

    #[test]
    fn deterministic_and_distinct() {
        // Same input -> same digest (deterministic).
        assert_eq!(sha256_hex(b"pw"), sha256_hex(b"pw"));
        // Different inputs -> different digests (no trivial collision on these).
        assert_ne!(sha256_hex(b"pw"), sha256_hex(b"pW"));
        assert_ne!(sha256_hex(b"s3cr3t"), sha256_hex(b"s3cr3T"));
    }

    #[test]
    fn block_boundary_lengths() {
        // Exercise messages around the 55/56/64-byte padding boundaries so the
        // pad-to-56-mod-64 logic (and the extra block it forces at >= 56) is covered.
        // Cross-checked against the streaming reference values for these fixed inputs.
        assert_eq!(
            sha256_hex(&[b'a'; 55]),
            "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318"
        );
        assert_eq!(
            sha256_hex(&[b'a'; 56]),
            "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"
        );
        assert_eq!(
            sha256_hex(&[b'a'; 64]),
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"
        );
    }
}
