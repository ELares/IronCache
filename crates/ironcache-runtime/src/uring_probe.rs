// SPDX-License-Identifier: MIT OR Apache-2.0
//! The io_uring STARTUP CAPABILITY PROBE + datapath selection (#284, IOURING_DATAPATH.md
//! "Multishot ops with one-shot fallback, chosen by startup probe").
//!
//! One Linux ARTIFACT must run across kernel versions: multishot recv needs kernel 6.0+, multishot
//! accept 5.19+, provided buffers 5.7+, while the baseline one-shot ring is 5.6+. Compiling the path
//! in with a `cfg` would fork the artifact per kernel; instead the backend PROBES the running kernel
//! once at startup ([`probe_uring_caps`]) and [`select_datapath`] picks the fastest path that kernel
//! actually supports. A kernel that lacks a capability transparently gets the slower-but-correct
//! path, never an `EINVAL` from submitting an unsupported opcode.
//!
//! Detection uses TWO mechanisms because io_uring exposes these capabilities two different ways.
//! Provided buffers (`PROVIDE_BUFFERS`) and fixed-buffer reads (`READ_FIXED`) are distinct OPCODES,
//! detected by `register_probe`. Multishot recv/accept are NOT opcodes -- they are base `RECV`/
//! `ACCEPT` with a flag bit -- so no opcode probe can see them (`RecvMulti::CODE == Recv::CODE`);
//! they are gated on the kernel VERSION instead (read from `/proc/sys/kernel/osrelease`). The
//! opcode-detected provided-buffer capability is also the real-capability BACKSTOP: the fast path is
//! selected only when the version says multishot AND the opcode probe confirms a buffer group, so a
//! restricted kernel that reports a new version with the buffer opcode masked still falls back safely.
//!
//! The split mirrors the rest of the runtime's Linux-only work: the DECISION ([`select_datapath`] +
//! the [`UringCaps`]/[`DataPath`] types) is pure and cfg-FREE, so it is truth-table unit-tested on
//! every host (including the macOS CI); the PROBE + its version-gating helpers are
//! `#[cfg(all(target_os = "linux", feature = "io_uring"))]`, with the version-boundary logic
//! truth-table tested on SYNTHETIC kernel versions (so all the 5.6/5.7/5.19/6.0 boundaries are
//! covered deterministically) and the real ring only smoke-tested (the CI io_uring datapath job + a
//! local Linux container). The fast datapaths this selects between are built ON this probe (a
//! following step); the probe is their compatibility gate.

/// The io_uring capabilities of the RUNNING kernel, as detected by [`probe_uring_caps`]. Each field
/// is "does this kernel support the opcode the corresponding datapath needs".
// This is a detected-CAPABILITY record: each bool is an independent kernel feature flag, so bools
// ARE the natural representation (not the boolean-blind API `struct_excessive_bools` warns about --
// there is no enum or bitflags that reads more clearly than one named bool per capability).
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UringCaps {
    /// `RECV` with `IORING_RECV_MULTISHOT` (kernel 6.0+): one SQE yields a CQE per arriving batch,
    /// removing per-read submission. Requires a provided-buffer group to deliver into.
    pub multishot_recv: bool,
    /// Multishot `ACCEPT` (kernel 5.19+): one SQE posts a CQE per new connection, cutting listener
    /// SQE churn.
    pub multishot_accept: bool,
    /// Provided buffers / buffer group (kernel 5.7+): the kernel picks a buffer from a group the
    /// shard registered and returns its id, removing the per-read buffer handoff. Multishot recv
    /// delivers into these.
    pub provided_buffers: bool,
    /// Fixed/registered-buffer read (`READ_FIXED`, kernel 5.1+ so effectively always when the ring
    /// exists): I/O into a pre-registered slab, removing the per-request pin/unpin.
    pub fixed_buffers: bool,
}

impl UringCaps {
    /// The BASELINE an io_uring host always has once the ring is created (kernel 5.6+): one-shot
    /// read/recv over owned buffers, no multishot / provided / fixed. Used as the conservative
    /// default and as a test fixture.
    #[must_use]
    pub fn baseline() -> Self {
        UringCaps {
            multishot_recv: false,
            multishot_accept: false,
            provided_buffers: false,
            fixed_buffers: false,
        }
    }
}

/// The datapath the io_uring backend runs on this kernel, fastest first (IOURING_DATAPATH.md). A
/// higher tier is chosen ONLY when every capability it needs is present, so the selection can never
/// submit an opcode the kernel lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPath {
    /// The full fast path: multishot recv delivering into a provided-buffer group over a
    /// registered slab. Needs `multishot_recv` AND `provided_buffers` (kernel 6.0+).
    MultishotProvided,
    /// The mid path: re-armed one-shot recv over FIXED registered buffers (no multishot, but the
    /// registered slab still removes the per-request pin/malloc). Needs `fixed_buffers`.
    OneShotFixed,
    /// The baseline: re-armed one-shot recv over OWNED buffers (the current substrate). Always
    /// available once the ring exists; the correct floor when nothing better is supported.
    OneShotOwned,
}

/// Pick the fastest [`DataPath`] the probed `caps` support (IOURING_DATAPATH.md path selection).
///
/// Pure + total, so it is truth-table tested on any host. The ordering is strict-superset-safe:
/// `MultishotProvided` requires both multishot recv and a provided-buffer group to deliver into
/// (multishot recv without provided buffers has nowhere to land, so it is NOT selected); the mid
/// tier needs only fixed buffers; else the always-available owned-buffer floor.
#[must_use]
pub fn select_datapath(caps: UringCaps) -> DataPath {
    if caps.multishot_recv && caps.provided_buffers {
        DataPath::MultishotProvided
    } else if caps.fixed_buffers {
        DataPath::OneShotFixed
    } else {
        DataPath::OneShotOwned
    }
}

/// Assemble [`UringCaps`] from the kernel VERSION (for the flag-gated multishot ops) and the
/// opcode-PROBE results (for the genuinely distinct provided/fixed-buffer opcodes). Pure, so the
/// version-boundary logic is truth-table tested on synthetic versions without needing many real
/// kernels.
///
/// CRITICAL (why the split): multishot recv/accept are NOT distinct io_uring opcodes -- they are
/// base `RECV`/`ACCEPT` with a FLAG (`IORING_RECV_MULTISHOT` / `IORING_ACCEPT_MULTISHOT`), so
/// `register_probe` (which is opcode-granular) CANNOT detect them: `RecvMulti::CODE == Recv::CODE`.
/// They must be gated on the kernel version instead (multishot recv landed in 6.0, multishot accept
/// in 5.19). Provided buffers (`PROVIDE_BUFFERS`, 5.7) and fixed-buffer reads (`READ_FIXED`, 5.1)
/// ARE distinct opcodes, so those stay opcode-detected -- which also makes them the real-capability
/// BACKSTOP: [`select_datapath`] requires BOTH `multishot_recv` (version) AND `provided_buffers`
/// (opcode) for the fast path, so a kernel that reports a new VERSION but has the buffer opcode
/// masked (a restricted/seccomp env) still falls back safely rather than submitting a doomed op.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[must_use]
fn caps_from_version_and_opcodes(
    major: u32,
    minor: u32,
    provided_buffers: bool,
    fixed_buffers: bool,
) -> UringCaps {
    let at_least = |a: u32, b: u32| (major, minor) >= (a, b);
    UringCaps {
        // IORING_RECV_MULTISHOT: kernel 6.0.
        multishot_recv: at_least(6, 0),
        // IORING_ACCEPT_MULTISHOT: kernel 5.19.
        multishot_accept: at_least(5, 19),
        provided_buffers,
        fixed_buffers,
    }
}

/// Parse a Linux `osrelease` string (e.g. `"6.8.0-117-generic"`) into `(major, minor)`. Returns
/// `None` for anything unparseable, which the probe treats CONSERVATIVELY (no multishot -- the safe
/// direction: a slower path, never an `EINVAL` from an unsupported flag).
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn parse_kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut fields = release.trim().split('.');
    let major = fields.next()?.parse().ok()?;
    let minor = fields.next()?.parse().ok()?;
    Some((major, minor))
}

/// Probe the RUNNING kernel's io_uring capabilities (IOURING_DATAPATH.md "startup feature probe").
///
/// Creates a small ring (which fails, `Err`, on a kernel without io_uring), asks it which OPCODES
/// it supports via `register_probe` (for the distinct provided/fixed-buffer opcodes), reads the
/// kernel VERSION from `/proc/sys/kernel/osrelease` (for the flag-gated multishot ops, which no
/// opcode probe can detect -- see [`caps_from_version_and_opcodes`]), and combines them. Returns
/// `Err` if io_uring is unavailable at all (kernel < 5.6, or `io_uring_disabled`): the caller then
/// uses the epoll/kqueue (tokio) backend rather than treating it as fatal, exactly as
/// [`crate::listen_fds`] treats a self-bind fallback.
///
/// # Errors
///
/// Returns the underlying `io::Error` if the ring cannot be created or the probe register fails
/// (io_uring absent/disabled) -- a signal to fall back, not a crash. An unreadable/malformed kernel
/// version is NOT an error: it degrades to the conservative no-multishot caps.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub fn probe_uring_caps() -> std::io::Result<UringCaps> {
    use io_uring::{IoUring, Probe, opcode};

    // A minimal ring purely to query support; it is dropped immediately. `new` fails on a kernel
    // without io_uring (or with it disabled), which is the fall-back-to-tokio signal.
    let ring = IoUring::new(8)?;
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;

    // The kernel version for the flag-gated multishot ops. A read/parse failure degrades to (0, 0)
    // = no multishot (the safe direction), never a hard error.
    let (major, minor) = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .and_then(|s| parse_kernel_version(&s))
        .unwrap_or((0, 0));

    Ok(caps_from_version_and_opcodes(
        major,
        minor,
        probe.is_supported(opcode::ProvideBuffers::CODE),
        probe.is_supported(opcode::ReadFixed::CODE),
    ))
}

#[cfg(test)]
mod tests {
    use super::{DataPath, UringCaps, select_datapath};

    #[test]
    fn select_datapath_picks_the_highest_supported_tier() {
        // Full fast path: multishot recv + provided buffers.
        let full = UringCaps {
            multishot_recv: true,
            multishot_accept: true,
            provided_buffers: true,
            fixed_buffers: true,
        };
        assert_eq!(select_datapath(full), DataPath::MultishotProvided);

        // No provided buffers -> multishot recv has nowhere to land -> drop to fixed.
        let no_provided = UringCaps {
            provided_buffers: false,
            ..full
        };
        assert_eq!(select_datapath(no_provided), DataPath::OneShotFixed);

        // Multishot recv present but NO provided buffers is not enough for the fast path (the
        // superset-safe rule): still fixed, not multishot.
        let recv_no_group = UringCaps {
            multishot_recv: true,
            multishot_accept: false,
            provided_buffers: false,
            fixed_buffers: true,
        };
        assert_eq!(select_datapath(recv_no_group), DataPath::OneShotFixed);

        // Only fixed buffers -> mid tier.
        let fixed_only = UringCaps {
            fixed_buffers: true,
            ..UringCaps::baseline()
        };
        assert_eq!(select_datapath(fixed_only), DataPath::OneShotFixed);

        // Nothing extra -> the always-available owned-buffer floor.
        assert_eq!(
            select_datapath(UringCaps::baseline()),
            DataPath::OneShotOwned
        );

        // Provided buffers WITHOUT multishot recv also falls to fixed (can't do multishot).
        let provided_no_multishot = UringCaps {
            provided_buffers: true,
            fixed_buffers: true,
            ..UringCaps::baseline()
        };
        assert_eq!(
            select_datapath(provided_no_multishot),
            DataPath::OneShotFixed
        );
    }

    // The version-gating + parsing + probe are Linux-only; their truth tables use SYNTHETIC kernel
    // versions, so all the multishot boundaries (5.6/5.7/5.19/6.0) are exercised deterministically
    // without needing many real kernels, and the real probe is only smoke-tested (the environment's
    // actual caps vary across CI runners, so specific caps are NOT asserted -- the detection LOGIC is
    // what the synthetic truth tables pin down).
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    mod linux {
        use super::super::{
            DataPath, caps_from_version_and_opcodes, parse_kernel_version, probe_uring_caps,
            select_datapath,
        };

        #[test]
        fn caps_from_version_gates_multishot_on_the_kernel() {
            // Below 5.19: no multishot at all (opcode caps pass through as given).
            let k5_6 = caps_from_version_and_opcodes(5, 6, true, true);
            assert!(!k5_6.multishot_recv && !k5_6.multishot_accept);
            assert!(k5_6.provided_buffers && k5_6.fixed_buffers);

            // 5.19: multishot ACCEPT lands, multishot recv still not (needs 6.0).
            let k5_19 = caps_from_version_and_opcodes(5, 19, true, true);
            assert!(!k5_19.multishot_recv, "recv needs 6.0");
            assert!(k5_19.multishot_accept, "accept lands at 5.19");

            // 6.0: multishot recv lands too.
            let k6_0 = caps_from_version_and_opcodes(6, 0, true, true);
            assert!(k6_0.multishot_recv && k6_0.multishot_accept);

            // A newer minor within 5.x does NOT reach 6.0 recv (tuple compare, not numeric minor).
            let k5_100 = caps_from_version_and_opcodes(5, 100, false, true);
            assert!(!k5_100.multishot_recv, "5.100 < 6.0");
            assert!(k5_100.multishot_accept, "5.100 >= 5.19");
            // Opcode caps are passed through verbatim (the backstop select_datapath relies on).
            assert!(!k5_100.provided_buffers && k5_100.fixed_buffers);
        }

        #[test]
        fn parse_kernel_version_handles_real_and_malformed_strings() {
            assert_eq!(parse_kernel_version("6.8.0-117-generic\n"), Some((6, 8)));
            assert_eq!(parse_kernel_version("5.19.2"), Some((5, 19)));
            assert_eq!(parse_kernel_version("6.1"), Some((6, 1)));
            assert_eq!(parse_kernel_version(""), None);
            assert_eq!(parse_kernel_version("garbage"), None);
            assert_eq!(parse_kernel_version("6.x.0"), None);
        }

        // SMOKE test on the real kernel: the probe must run without panicking and, when io_uring is
        // present, produce caps whose selected path is internally coherent. Specific caps are NOT
        // asserted (CI runners restrict io_uring differently); the detection logic is pinned by the
        // synthetic truth tables above.
        #[test]
        fn probe_runs_and_selects_a_coherent_path_on_a_real_ring() {
            // No io_uring here (kernel < 5.6 / disabled / seccomp): correct Err fall-back signal.
            let Ok(caps) = probe_uring_caps() else {
                return;
            };
            // The fast path is only ever selected when BOTH its requirements were detected -- the
            // safety invariant, robust regardless of which caps this particular runner exposes.
            if select_datapath(caps) == DataPath::MultishotProvided {
                assert!(
                    caps.multishot_recv && caps.provided_buffers,
                    "fast path selected without both requirements: {caps:?}"
                );
            }
        }
    }
}
