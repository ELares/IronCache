// SPDX-License-Identifier: MIT OR Apache-2.0
//! Determinism seam for IronCache (ADR-0003, invariant 2).
//!
//! Every source of nondeterminism (the monotonic clock, the wall clock, and the
//! RNG) is funneled through the [`Env`] trait so a seeded replay is byte-identical
//! [dst-fdb-tigerbeetle-single-seed]. No code on a decision path calls
//! `std::time`, `Instant::now`, `SystemTime::now`, or `rand` directly; it goes
//! through `Env`. This crate is the ONE place real time and OS entropy are
//! allowed, and only inside [`SystemEnv`]; library crates depend on the trait,
//! never on the system implementation.
//!
//! ## Freeze point
//!
//! The trait surface here is a freeze point: downstream crates (runtime, server,
//! store, eviction, expiration) are generic over `Env` and depend on these exact
//! signatures. The surface is intentionally minimal:
//!
//! - [`Clock::now`] - a monotonic instant as a [`Monotonic`] newtype. Used for
//!   durations, idle-timeout accounting, and latency. It never goes backwards.
//! - [`Clock::now_unix_millis`] - wall-clock milliseconds since the Unix epoch.
//!   Used for absolute TTL (`EXPIREAT`/`PEXPIREAT`) and INFO. May jump.
//! - [`Rng::next_u64`] - the single entropy primitive; everything else
//!   (bounded ints, IDs, jitter) is derived from it deterministically.
//!
//! [`TestEnv`] provides a manually advanced clock and a seeded, reproducible RNG
//! for deterministic tests and DST (#95/#160).

#![forbid(unsafe_code)]

use core::time::Duration;

/// A monotonic instant, opaque and comparable, produced by [`Clock::now`].
///
/// It is a newtype over a [`Duration`] measured from an implementation-defined
/// but fixed origin (process start for [`SystemEnv`], zero for [`TestEnv`]). Only
/// differences between two `Monotonic` values are meaningful; the absolute value
/// is not a wall-clock time. This is deliberately not convertible to or from
/// `std::time::Instant` so the seam cannot leak real time into library crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Monotonic(Duration);

impl Monotonic {
    /// The origin instant (zero elapsed). Useful as a sentinel and in tests.
    pub const ZERO: Monotonic = Monotonic(Duration::ZERO);

    /// Construct from a duration since the clock origin. Intended for the `Env`
    /// implementations and tests, not for decision-path code.
    #[inline]
    pub const fn from_since_origin(d: Duration) -> Monotonic {
        Monotonic(d)
    }

    /// The elapsed time since the clock origin.
    #[inline]
    pub const fn since_origin(self) -> Duration {
        self.0
    }

    /// The duration from `earlier` to `self`, saturating at zero if `earlier`
    /// is later (a monotonic clock should never produce that, but the saturating
    /// form keeps callers panic-free).
    #[inline]
    pub fn saturating_duration_since(self, earlier: Monotonic) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// `self + d`, saturating at the representable maximum.
    #[inline]
    #[must_use]
    pub fn saturating_add(self, d: Duration) -> Monotonic {
        Monotonic(self.0.saturating_add(d))
    }
}

/// The monotonic and wall clocks, behind the determinism seam.
pub trait Clock {
    /// A monotonic, never-decreasing instant. The basis for all duration and
    /// idle-timeout math (CONNECTION_LIFECYCLE.md) and latency measurement.
    fn now(&self) -> Monotonic;

    /// Wall-clock milliseconds since the Unix epoch. The basis for absolute TTL
    /// and INFO `uptime`/time fields. May move non-monotonically.
    fn now_unix_millis(&self) -> u64;
}

/// A deterministic random source behind the determinism seam.
///
/// `next_u64` is the single primitive; the provided methods derive everything
/// else from it so two implementations agreeing on `next_u64` agree on all of
/// them. A seeded implementation must be reproducible: same seed, same sequence.
pub trait Rng {
    /// The next 64 bits of the stream.
    fn next_u64(&mut self) -> u64;

    /// A uniform `u64` in `[0, bound)`. Returns `0` when `bound == 0`. Uses
    /// Lemire's multiply-shift (no modulo bias) so the mapping is stable across
    /// implementations that share `next_u64`.
    #[inline]
    fn gen_below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // Widening-multiply rejection-free reduction; bias is bounded and
        // deterministic, which is what DST needs (not cryptographic uniformity).
        let m = u128::from(self.next_u64()) * u128::from(bound);
        (m >> 64) as u64
    }

    /// A `f64` in `[0.0, 1.0)` derived from the top 53 bits of `next_u64`.
    #[inline]
    fn gen_unit_f64(&mut self) -> f64 {
        // 53 bits = f64 mantissa; division by 2^53 lands in [0, 1).
        let bits = self.next_u64() >> 11;
        (bits as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

/// The full environment seam: a clock plus an RNG.
///
/// Library crates take an `&E: Env` (or `&mut` for RNG-bearing paths) rather than
/// touching the OS. The binary constructs exactly one [`SystemEnv`]; tests
/// construct a [`TestEnv`].
pub trait Env: Clock {
    /// The concrete RNG type this environment hands out.
    type Rng: Rng;

    /// Borrow a mutable handle to the environment's RNG.
    fn rng(&mut self) -> &mut Self::Rng;
}

// ---------------------------------------------------------------------------
// SystemEnv: the ONLY real-time / OS-entropy implementation.
// ---------------------------------------------------------------------------

/// The production environment. This is the single place in the codebase allowed
/// to read `std::time` and OS entropy; the invariant lint enforces that no other
/// crate does. Constructed once, at the binary edge, with [`SystemEnv::new`].
#[derive(Debug)]
pub struct SystemEnv {
    origin: std::time::Instant,
    rng: SplitMix64,
}

impl SystemEnv {
    /// Construct a `SystemEnv`, fixing the monotonic origin at `now` and seeding
    /// the RNG from the wall clock (the seed is non-decision-path: it only
    /// affects values that are themselves nondeterministic in production, e.g.
    /// the client-id jitter). DST uses [`TestEnv`] with a fixed seed instead.
    #[must_use]
    pub fn new() -> Self {
        // The wall clock seed is fine here: SystemEnv is non-deterministic by
        // definition, and this is the sanctioned boundary (ADR-0003).
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0xA076_1D64_78BD_642F, |d| d.as_nanos() as u64);
        SystemEnv {
            origin: std::time::Instant::now(),
            rng: SplitMix64::new(seed),
        }
    }
}

impl Default for SystemEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemEnv {
    #[inline]
    fn now(&self) -> Monotonic {
        Monotonic(self.origin.elapsed())
    }

    #[inline]
    fn now_unix_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

impl Env for SystemEnv {
    type Rng = SplitMix64;

    #[inline]
    fn rng(&mut self) -> &mut Self::Rng {
        &mut self.rng
    }
}

// ---------------------------------------------------------------------------
// TestEnv: manually advanced clock, seeded deterministic RNG.
// ---------------------------------------------------------------------------

/// A fully deterministic environment for tests and DST. The clock advances only
/// when [`TestEnv::advance`] (or [`TestEnv::set_unix_millis`]) is called, and the
/// RNG is a seeded [`SplitMix64`], so a run is byte-reproducible from its seed.
#[derive(Debug)]
pub struct TestEnv {
    mono: Duration,
    unix_millis: u64,
    rng: SplitMix64,
}

impl TestEnv {
    /// A `TestEnv` seeded with `seed`, clock at origin (monotonic zero) and a
    /// wall clock of zero.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        TestEnv {
            mono: Duration::ZERO,
            unix_millis: 0,
            rng: SplitMix64::new(seed),
        }
    }

    /// Advance the monotonic clock (and the wall clock by the same delta) by `d`.
    pub fn advance(&mut self, d: Duration) {
        self.mono = self.mono.saturating_add(d);
        self.unix_millis = self
            .unix_millis
            .saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    }

    /// Set the absolute wall clock without touching the monotonic clock.
    pub fn set_unix_millis(&mut self, millis: u64) {
        self.unix_millis = millis;
    }
}

impl Clock for TestEnv {
    #[inline]
    fn now(&self) -> Monotonic {
        Monotonic(self.mono)
    }

    #[inline]
    fn now_unix_millis(&self) -> u64 {
        self.unix_millis
    }
}

impl Env for TestEnv {
    type Rng = SplitMix64;

    #[inline]
    fn rng(&mut self) -> &mut Self::Rng {
        &mut self.rng
    }
}

// ---------------------------------------------------------------------------
// SplitMix64: the deterministic RNG primitive used by both Env implementations.
// ---------------------------------------------------------------------------

/// A small, fast, fully deterministic RNG (`SplitMix64`). It is not
/// cryptographic; it is used for jitter, ghost-queue sampling, and DST replay,
/// where reproducibility (not unpredictability) is the requirement. Same seed
/// produces the same stream on every platform.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seed the generator. Any `u64` is a valid seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }
}

impl Rng for SplitMix64 {
    #[inline]
    fn next_u64(&mut self) -> u64 {
        // The canonical splitmix64 from Steele/Lea/Flood, public domain.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_clock_advances_only_on_request() {
        let mut env = TestEnv::new(1);
        assert_eq!(env.now(), Monotonic::ZERO);
        assert_eq!(env.now_unix_millis(), 0);
        env.advance(Duration::from_millis(1500));
        assert_eq!(env.now().since_origin(), Duration::from_millis(1500));
        assert_eq!(env.now_unix_millis(), 1500);
        // Idempotent reads: the clock does not move on its own.
        assert_eq!(env.now().since_origin(), Duration::from_millis(1500));
    }

    #[test]
    fn monotonic_duration_math_saturates() {
        let a = Monotonic::from_since_origin(Duration::from_secs(10));
        let b = Monotonic::from_since_origin(Duration::from_secs(3));
        assert_eq!(a.saturating_duration_since(b), Duration::from_secs(7));
        // earlier-after-later saturates to zero rather than panicking.
        assert_eq!(b.saturating_duration_since(a), Duration::ZERO);
    }

    #[test]
    fn seeded_rng_is_reproducible() {
        let mut a = SplitMix64::new(0xDEAD_BEEF);
        let mut b = SplitMix64::new(0xDEAD_BEEF);
        let seq_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b);
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn splitmix64_known_vector() {
        // First output for seed 0 is a stable, well-known splitmix64 value; it
        // pins the algorithm so a refactor cannot silently change the stream.
        let mut r = SplitMix64::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
    }

    #[test]
    fn gen_below_respects_bound_and_zero() {
        let mut r = SplitMix64::new(42);
        for _ in 0..1000 {
            assert!(r.gen_below(10) < 10);
        }
        assert_eq!(r.gen_below(0), 0);
        assert_eq!(r.gen_below(1), 0);
    }

    #[test]
    fn gen_unit_f64_in_range() {
        let mut r = SplitMix64::new(7);
        for _ in 0..1000 {
            let x = r.gen_unit_f64();
            assert!((0.0..1.0).contains(&x));
        }
    }

    #[test]
    fn test_env_rng_seeded_via_env_trait() {
        let mut e1 = TestEnv::new(99);
        let mut e2 = TestEnv::new(99);
        assert_eq!(e1.rng().next_u64(), e2.rng().next_u64());
    }
}
