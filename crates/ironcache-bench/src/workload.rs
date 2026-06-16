// SPDX-License-Identifier: MIT OR Apache-2.0
//! The seeded workload: a YCSB/Gray-style zipfian key generator plus a read/write
//! op-mix and a deterministic key/value encoder.
//!
//! ## Zipfian generation (Gray et al., O(1) per draw)
//!
//! The key popularity follows a zipfian distribution with exponent `theta`: a few
//! hot keys dominate while a long tail is rarely touched, which is the realistic
//! cache access pattern (and the one YCSB workloads model). We use the O(1)
//! scrambled-zipfian method from Gray, Sundaresan, Englert, Baclawski and Weinberger,
//! "Quickly Generating Billion-Record Synthetic Databases" (SIGMOD 1994). The
//! generalized-harmonic constant `zeta(n, theta) = sum_{i=1..n} 1/i^theta` and a few
//! derived constants are precomputed ONCE in [`Zipf::new`]; each [`Zipf::next`] is
//! then a constant number of `powf`/comparison operations using a single uniform
//! `[0,1)` draw, with no per-draw loop over the keyspace.
//!
//! This is the same closed-form inversion YCSB's `ZipfianGenerator` uses (it cites
//! the identical Gray et al. paper). It is deterministic given the RNG seed: a fixed
//! seed produces a byte-reproducible sequence of draws.
//!
//! ## Determinism (ADR-0003, invariant 2)
//!
//! All randomness flows through a seeded [`ironcache_env::SplitMix64`] handed in by
//! the caller (the load generator constructs one per logical stream from
//! `seed + stream_index`). No clock, no OS entropy, no `rand` crate.

#![forbid(unsafe_code)]

use ironcache_env::Rng;

/// The two operations the workload issues against the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// A `GET key` for the given zero-based key index.
    Get(u64),
    /// A `SET key value` for the given zero-based key index.
    Set(u64),
}

impl Op {
    /// The key index this op touches, regardless of variant.
    #[must_use]
    pub fn key_index(self) -> u64 {
        match self {
            Op::Get(k) | Op::Set(k) => k,
        }
    }

    /// `true` for [`Op::Get`] (a read), `false` for [`Op::Set`] (a write).
    #[must_use]
    pub fn is_read(self) -> bool {
        matches!(self, Op::Get(_))
    }
}

/// An O(1) zipfian index generator over `[0, n)` with exponent `theta`, using the
/// Gray et al. (SIGMOD 1994) closed-form method. Deterministic given the RNG seed.
///
/// The distribution is "scrambled" in the sense that index 0 is the most popular
/// and popularity decays as `1/(rank+1)^theta`; the caller is free to hash the
/// returned index into the keyspace if a non-clustered hot set is desired, but for
/// reproducible benchmarking the raw rank-ordered index is what we want (the hot
/// keys are a known, contiguous low-index set, which makes the skew assertions in
/// the tests checkable).
#[derive(Debug, Clone)]
pub struct Zipf {
    /// Keyspace size (number of distinct indices).
    n: u64,
    /// The exponent. Larger theta means more skew; theta -> 0 approaches uniform.
    theta: f64,
    /// `zeta(n, theta) = sum_{i=1..n} 1/i^theta`, the generalized harmonic number.
    zeta_full: f64,
    /// `1 - theta`, reused in the inversion.
    one_minus_theta: f64,
    /// `(1 - zeta_two/zeta_full)`, the Gray et al. `eta` constant.
    eta: f64,
    /// `0.5^theta`, reused in the inversion.
    half_pow_theta: f64,
}

impl Zipf {
    /// Construct a zipfian generator over `[0, n)` with exponent `theta`.
    ///
    /// Precomputes `zeta(n, theta)` and the derived Gray et al. constants once
    /// (an O(n) sum here, then O(1) per draw). `n` is clamped to at least 1 and
    /// `theta` to a small positive floor so the constants are finite.
    #[must_use]
    pub fn new(n: u64, theta: f64) -> Self {
        let n = n.max(1);
        // Guard against theta == 1 (the harmonic series; the closed form divides by
        // (1 - theta)) and against non-positive theta. The locked default is 0.99.
        let theta = if theta <= 0.0 {
            0.01
        } else if (theta - 1.0).abs() < 1e-9 {
            // Nudge off the singularity; 0.9999 is visually 1.0 but keeps 1-theta != 0.
            0.9999
        } else {
            theta
        };
        let zeta_full = Self::zeta(n, theta);
        let zeta_two = Self::zeta(2, theta);
        let one_minus_theta = 1.0 - theta;
        let half_pow_theta = 0.5_f64.powf(theta);
        // eta = (1 - (2/n)^(1-theta)) / (1 - zeta(2,theta)/zeta(n,theta))
        let eta = (1.0 - (2.0 / n as f64).powf(one_minus_theta)) / (1.0 - zeta_two / zeta_full);
        Zipf {
            n,
            theta,
            zeta_full,
            one_minus_theta,
            eta,
            half_pow_theta,
        }
    }

    /// `zeta(n, theta) = sum_{i=1..n} 1/i^theta`, the generalized harmonic number.
    /// Computed once in [`Zipf::new`]; this is the only O(n) step.
    fn zeta(n: u64, theta: f64) -> f64 {
        let mut sum = 0.0_f64;
        for i in 1..=n {
            sum += 1.0 / (i as f64).powf(theta);
        }
        sum
    }

    /// The keyspace size.
    #[must_use]
    pub fn keyspace(&self) -> u64 {
        self.n
    }

    /// Draw the next zipfian index in `[0, n)` using one uniform `[0,1)` draw.
    ///
    /// This is the Gray et al. closed-form inversion (the same one YCSB uses):
    /// it maps a uniform `u` to a rank via two branches around the `eta`/`zeta`
    /// constants, with no loop over the keyspace. O(1).
    pub fn next<R: Rng>(&self, rng: &mut R) -> u64 {
        let u = rng.gen_unit_f64();
        let uz = u * self.zeta_full;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + self.half_pow_theta {
            return 1.min(self.n - 1);
        }
        // ret = n * (eta*u - eta + 1)^(1/(1-theta))
        let exponent = 1.0 / self.one_minus_theta;
        let inner = self.eta * u - self.eta + 1.0;
        let ret = (self.n as f64) * inner.powf(exponent);
        // Clamp into range; powf rounding can nudge slightly past the ends.
        let idx = ret as u64;
        idx.min(self.n - 1)
    }
}

/// The full workload configuration: keyspace, skew, op-mix, and value size.
///
/// Cloned cheaply into each connection task; the per-stream RNG is separate (so two
/// streams seeded from `seed + i` draw independent-but-reproducible sequences).
#[derive(Debug, Clone)]
pub struct Workload {
    zipf: Zipf,
    /// Fraction of ops that are reads (GET). `0.9` => 90% GET / 10% SET.
    read_ratio: f64,
    /// The fixed byte length of a SET value (locked range 64..=1024).
    value_size: usize,
}

/// The locked-in minimum and maximum value size (BENCHMARK.md: the value-size knob
/// is bounded to keep runs comparable). The default is 128.
pub const VALUE_SIZE_MIN: usize = 64;
/// See [`VALUE_SIZE_MIN`].
pub const VALUE_SIZE_MAX: usize = 1024;

impl Workload {
    /// Build a workload over a keyspace of `keyspace` keys with zipf exponent
    /// `theta`, a `read_ratio` in `[0,1]`, and a `value_size` clamped to the locked
    /// `64..=1024` range.
    #[must_use]
    pub fn new(keyspace: u64, theta: f64, read_ratio: f64, value_size: usize) -> Self {
        Workload {
            zipf: Zipf::new(keyspace, theta),
            read_ratio: read_ratio.clamp(0.0, 1.0),
            value_size: value_size.clamp(VALUE_SIZE_MIN, VALUE_SIZE_MAX),
        }
    }

    /// The configured read ratio (after clamping).
    #[must_use]
    pub fn read_ratio(&self) -> f64 {
        self.read_ratio
    }

    /// The effective zipf exponent (after the [`Zipf::new`] guards).
    #[must_use]
    pub fn theta(&self) -> f64 {
        self.zipf.theta
    }

    /// The configured value size (after clamping).
    #[must_use]
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// The keyspace size.
    #[must_use]
    pub fn keyspace(&self) -> u64 {
        self.zipf.keyspace()
    }

    /// Draw the next op: a zipfian key index plus a read-vs-write decision from a
    /// separate uniform draw. Reads draw first (key), then the coin, so the stream
    /// is deterministic and the two draws do not alias.
    pub fn next_op<R: Rng>(&self, rng: &mut R) -> Op {
        let key = self.zipf.next(rng);
        let coin = rng.gen_unit_f64();
        if coin < self.read_ratio {
            Op::Get(key)
        } else {
            Op::Set(key)
        }
    }

    /// The deterministic key bytes for index `idx`: `b"k:" + decimal(idx)`. Stable,
    /// ASCII, no RNG; the same idx always maps to the same key bytes.
    #[must_use]
    pub fn key_bytes(&self, idx: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + 20);
        k.extend_from_slice(b"k:");
        k.extend_from_slice(idx.to_string().as_bytes());
        k
    }

    /// A fixed value of `value_size` bytes (a repeated glyph). The value is the same
    /// for every SET; the benchmark measures the cache, not value diversity.
    #[must_use]
    pub fn value_bytes(&self) -> Vec<u8> {
        vec![b'v'; self.value_size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_env::SplitMix64;

    #[test]
    fn zipf_is_skewed_toward_low_indices() {
        // Over many draws, the single most-frequent index must be a LOW index and
        // the head must dominate the tail. theta 0.99 over a 1000-key space.
        let zipf = Zipf::new(1000, 0.99);
        let mut rng = SplitMix64::new(0xC0FF_EE00);
        let mut counts = vec![0u64; 1000];
        let draws = 200_000u64;
        for _ in 0..draws {
            counts[zipf.next(&mut rng) as usize] += 1;
        }
        // The argmax index is in the low band (top 1% of the keyspace).
        let (argmax, &maxc) = counts.iter().enumerate().max_by_key(|&(_, c)| *c).unwrap();
        assert!(
            argmax < 10,
            "most-frequent index {argmax} should be a low index (count {maxc})"
        );
        // Frequency decreases head-to-tail: the top decile is hit far more than the
        // bottom decile.
        let head: u64 = counts[..100].iter().sum();
        let tail: u64 = counts[900..].iter().sum();
        assert!(
            head > tail * 5,
            "head {head} must dominate tail {tail} under skew"
        );
        // The very first index is the single hottest (count is monotone-ish at the head).
        assert!(counts[0] >= counts[50], "index 0 should beat a mid index");
    }

    #[test]
    fn zipf_is_reproducible_for_a_fixed_seed() {
        let zipf = Zipf::new(10_000, 0.99);
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        let seq_a: Vec<u64> = (0..256).map(|_| zipf.next(&mut a)).collect();
        let seq_b: Vec<u64> = (0..256).map(|_| zipf.next(&mut b)).collect();
        assert_eq!(seq_a, seq_b, "same seed must give the same zipf draws");
    }

    #[test]
    fn zipf_draws_stay_in_range() {
        let zipf = Zipf::new(64, 1.2);
        let mut rng = SplitMix64::new(7);
        for _ in 0..50_000 {
            assert!(zipf.next(&mut rng) < 64);
        }
    }

    #[test]
    fn zipf_handles_tiny_and_degenerate_keyspace() {
        // n = 1 always yields 0.
        let one = Zipf::new(1, 0.99);
        let mut rng = SplitMix64::new(1);
        for _ in 0..100 {
            assert_eq!(one.next(&mut rng), 0);
        }
        // theta near 1 (the singularity) is nudged off and still produces in-range draws.
        let near_one = Zipf::new(100, 1.0);
        for _ in 0..1000 {
            assert!(near_one.next(&mut rng) < 100);
        }
    }

    #[test]
    fn op_mix_matches_read_ratio() {
        // Over many draws the empirical read fraction is close to read_ratio.
        let wl = Workload::new(1000, 0.99, 0.9, 128);
        let mut rng = SplitMix64::new(0xABCD);
        let draws = 100_000u64;
        let mut reads = 0u64;
        for _ in 0..draws {
            if wl.next_op(&mut rng).is_read() {
                reads += 1;
            }
        }
        let frac = reads as f64 / draws as f64;
        assert!(
            (frac - 0.9).abs() < 0.02,
            "read fraction {frac} should be ~0.9"
        );
    }

    #[test]
    fn op_mix_extremes_are_pure() {
        let mut rng = SplitMix64::new(5);
        let all_reads = Workload::new(100, 0.99, 1.0, 128);
        let all_writes = Workload::new(100, 0.99, 0.0, 128);
        for _ in 0..1000 {
            assert!(all_reads.next_op(&mut rng).is_read());
            assert!(!all_writes.next_op(&mut rng).is_read());
        }
    }

    #[test]
    fn key_and_value_encoding_is_deterministic_and_sized() {
        let wl = Workload::new(1000, 0.99, 0.9, 256);
        assert_eq!(wl.key_bytes(0), b"k:0");
        assert_eq!(wl.key_bytes(12345), b"k:12345");
        assert_eq!(wl.key_bytes(7), wl.key_bytes(7));
        assert_ne!(wl.key_bytes(1), wl.key_bytes(2));
        assert_eq!(wl.value_bytes().len(), 256);
    }

    #[test]
    fn value_size_is_clamped_to_locked_range() {
        assert_eq!(Workload::new(1, 0.99, 0.9, 1).value_size(), VALUE_SIZE_MIN);
        assert_eq!(
            Workload::new(1, 0.99, 0.9, 99_999).value_size(),
            VALUE_SIZE_MAX
        );
        assert_eq!(Workload::new(1, 0.99, 0.9, 128).value_size(), 128);
    }
}
