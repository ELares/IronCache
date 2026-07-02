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
//! The split mirrors the rest of the runtime's Linux-only work: the DECISION ([`select_datapath`] +
//! the [`UringCaps`]/[`DataPath`] types) is pure and cfg-FREE, so it is truth-table unit-tested on
//! every host (including the macOS CI); the actual PROBE ([`probe_uring_caps`], which creates a real
//! ring and calls `register_probe`) is `#[cfg(all(target_os = "linux", feature = "io_uring"))]`,
//! validated on a real kernel (the CI io_uring datapath job + a local Linux container). The fast
//! datapaths this selects between are built ON this probe (a following step); the probe is their
//! compatibility gate.

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

/// Probe the RUNNING kernel's io_uring capabilities (IOURING_DATAPATH.md "startup feature probe").
///
/// Creates a small ring and asks the kernel, via `register_probe`, which opcodes it supports, then
/// maps the ones the datapaths need onto [`UringCaps`]. Returns `Err` if io_uring is unavailable at
/// all (kernel < 5.6, or `io_uring_disabled`): the caller then uses the epoll/kqueue (tokio) backend
/// rather than treating it as fatal, exactly as [`crate::listen_fds`] treats a self-bind fallback.
///
/// # Errors
///
/// Returns the underlying `io::Error` if the ring cannot be created or the probe register fails
/// (io_uring absent/disabled) -- a signal to fall back, not a crash.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub fn probe_uring_caps() -> std::io::Result<UringCaps> {
    use io_uring::{IoUring, Probe, opcode};

    // A minimal ring purely to query support; it is dropped immediately. `new` fails on a kernel
    // without io_uring (or with it disabled), which is the fall-back-to-tokio signal.
    let ring = IoUring::new(8)?;
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;

    Ok(UringCaps {
        multishot_recv: probe.is_supported(opcode::RecvMulti::CODE),
        multishot_accept: probe.is_supported(opcode::AcceptMulti::CODE),
        provided_buffers: probe.is_supported(opcode::ProvideBuffers::CODE),
        fixed_buffers: probe.is_supported(opcode::ReadFixed::CODE),
    })
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

    // The real probe runs only on Linux with the feature (a real ring): assert the running kernel
    // reports a coherent capability set and that the selected path is well-formed. On the CI
    // io_uring runner + a modern local container (kernel 6.x) this exercises the true detection.
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    #[test]
    fn probe_reports_coherent_caps_on_this_kernel() {
        // A kernel without io_uring: the probe correctly errors (fall-back signal), nothing to
        // assert about caps, so just return.
        let Ok(caps) = super::probe_uring_caps() else {
            return;
        };
        // Non-vacuous: `READ_FIXED` has existed since io_uring's first release (5.1), so any kernel
        // that could CREATE the ring (5.6+, i.e. the probe returned Ok) supports it. This proves the
        // probe actually detected real opcode support, not an all-false default.
        assert!(
            caps.fixed_buffers,
            "READ_FIXED must be supported on every io_uring-capable kernel: {caps:?}"
        );
        // multishot recv REQUIRES a provided-buffer group to be useful; any kernel new enough to
        // support multishot recv (6.0+) also supports provided buffers (5.7+), so this implication
        // must hold on every real kernel.
        assert!(
            !caps.multishot_recv || caps.provided_buffers,
            "multishot recv without provided buffers is an incoherent kernel report: {caps:?}"
        );
        // The selected path is always one of the three tiers and never asks for an unsupported op.
        let path = select_datapath(caps);
        if path == DataPath::MultishotProvided {
            assert!(caps.multishot_recv && caps.provided_buffers);
        }
    }
}
