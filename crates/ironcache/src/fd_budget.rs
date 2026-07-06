// SPDX-License-Identifier: MIT OR Apache-2.0
//! Boot-time `RLIMIT_NOFILE` budgeting (#532, Redis `adjustOpenFilesLimit` parity).
//!
//! A node configured with a high `maxclients` on a box with a low `ulimit -n` will
//! run out of file descriptors MID-TRAFFIC and start returning `EMFILE` on accept,
//! rather than cleanly bounding the connection count at boot. At startup we read the
//! open-file soft/hard limits, RAISE the soft limit toward the hard limit where the
//! kernel lets us (so the requested `maxclients` fits), and otherwise CLAMP the
//! effective `maxclients` down to what the fd budget allows, with a LOUD warning that
//! names the requested ceiling, the limit, and the clamped value (the operator then
//! knows to raise `ulimit -n` / `LimitNOFILE=` to restore the requested ceiling).
//!
//! This is boot / OS-seam code OUTSIDE the determinism boundary (ADR-0003): it runs
//! once in the binary before any shard is wired, so the libc `getrlimit`/`setrlimit`
//! syscalls are fine here. The BUDGET MATH is extracted into the pure, unit-tested
//! [`compute_fd_budget`]; only the thin `#[cfg(unix)]` wrappers touch libc. On a
//! non-unix target the whole thing is a logged no-op.

use ironcache_config::Config;

/// The file descriptors reserved for the process itself, on TOP of the client
/// connections: the per-shard SO_REUSEPORT RESP listeners, the optional TLS +
/// metrics-HTTP + unix-socket listeners, the persistence snapshot/manifest temp
/// files, and the cluster-bus + raft-net inter-node connections. Mirrors Redis's
/// fixed `CONFIG_MIN_RESERVED_FDS` headroom (32) with extra slack for our additional
/// listeners; a static baseline, tunable later if the seam ever needs it (#532).
// Reachable from `apply_fd_budget` only under `#[cfg(unix)]`; on a non-unix build the
// pure math is exercised only by the unit tests, so allow it to be otherwise unused.
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) const RESERVED_FDS: u64 = 64;

/// The decision produced by [`compute_fd_budget`]: the effective `maxclients` after
/// budgeting, plus the soft limit to raise `RLIMIT_NOFILE` to (if any).
#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FdBudget {
    /// The `maxclients` ceiling that fits the fd budget. Equal to the requested value
    /// when it fits (possibly after raising the soft limit), or CLAMPED below it when
    /// even the hard limit cannot cover `requested + reserved`. `0` stays `0`
    /// (unlimited is never clamped).
    pub(crate) effective_maxclients: u64,
    /// `Some(target)` when the soft limit should be raised to `target` fds (always a
    /// strict increase over the current soft limit); `None` when no raise is needed or
    /// possible. "Whether to raise" is `raise_soft_to.is_some()`.
    pub(crate) raise_soft_to: Option<u64>,
}

/// PURE budget math (no syscalls): given the current `RLIMIT_NOFILE` `soft`/`hard`
/// limits, the `requested` `maxclients`, and the `reserved` process headroom, decide
/// the effective `maxclients` and whether/where to raise the soft limit. This is the
/// unit-tested core of the boot logic (#532); the caller performs the actual
/// `setrlimit`/`getrlimit` around it.
///
/// Semantics (Redis `adjustOpenFilesLimit` parity):
/// - `requested == 0` means UNLIMITED (the cap is disabled): never clamp, but still
///   raise the soft limit toward the hard limit to maximize headroom.
/// - Otherwise we need `requested + reserved` fds. If the soft limit already covers
///   that, do nothing. If the HARD limit covers it, ask to raise the soft limit to
///   exactly that. If not even the hard limit covers it, ask to raise the soft limit
///   as far as it goes (the hard limit) and CLAMP `maxclients` to `hard - reserved`.
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) fn compute_fd_budget(soft: u64, hard: u64, requested: u64, reserved: u64) -> FdBudget {
    // Unlimited: nothing to clamp. Raise the soft limit toward the hard limit where
    // that is a real increase, so an unlimited node still gets the most fds it can.
    if requested == 0 {
        return FdBudget {
            effective_maxclients: 0,
            raise_soft_to: (hard > soft).then_some(hard),
        };
    }

    let required = requested.saturating_add(reserved);

    // The soft limit already covers the requested ceiling plus headroom: no change.
    if soft >= required {
        return FdBudget {
            effective_maxclients: requested,
            raise_soft_to: None,
        };
    }

    // The hard limit covers it: raise the soft limit to exactly what we need. Because
    // `soft < required <= hard`, this is always a strict increase, so no clamp.
    if hard >= required {
        return FdBudget {
            effective_maxclients: requested,
            raise_soft_to: Some(required),
        };
    }

    // Even the hard limit is short. Raise the soft limit as far as the hard limit
    // allows (only if that actually increases it), and clamp `maxclients` to the fds
    // that remain after the reserved headroom (saturating to `0` if the hard limit is
    // below the headroom itself).
    FdBudget {
        effective_maxclients: hard.saturating_sub(reserved),
        raise_soft_to: (hard > soft).then_some(hard),
    }
}

/// Apply the boot-time `RLIMIT_NOFILE` budget to `cfg` (#532). Reads the current
/// open-file limits, raises the soft limit where the kernel allows, and CLAMPS
/// `cfg.maxclients` down (with a LOUD warning) when the fd budget cannot cover the
/// requested ceiling. A no-op (debug-logged) on non-unix targets.
pub(crate) fn apply_fd_budget(cfg: &mut Config) {
    #[cfg(unix)]
    {
        let requested = cfg.maxclients;
        let Some((soft, hard)) = current_nofile_limits() else {
            tracing::warn!("could not read RLIMIT_NOFILE; leaving maxclients unchanged");
            return;
        };

        let budget = compute_fd_budget(soft, hard, requested, RESERVED_FDS);

        // Raise the soft limit if asked; track the limit we actually end up with so a
        // failed `setrlimit` still clamps to the truth rather than the wished-for value.
        let mut soft_now = soft;
        if let Some(target) = budget.raise_soft_to {
            match set_soft_nofile(target, hard) {
                Ok(()) => {
                    soft_now = target;
                    tracing::info!(
                        soft_was = soft,
                        soft_now = target,
                        hard_limit = hard,
                        "raised the open-file soft limit (RLIMIT_NOFILE) to fit maxclients plus reserved fds"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        target,
                        hard_limit = hard,
                        "could not raise the open-file soft limit; clamping maxclients to the current limit"
                    );
                }
            }
        }

        // Recompute the effective ceiling against the soft limit we ACTUALLY achieved
        // (pass it as both soft and hard so no further raise is assumed). This yields
        // the requested value when it now fits, or the clamped value otherwise.
        let effective =
            compute_fd_budget(soft_now, soft_now, requested, RESERVED_FDS).effective_maxclients;
        if effective < requested {
            tracing::warn!(
                requested_maxclients = requested,
                soft_limit = soft_now,
                hard_limit = hard,
                reserved_fds = RESERVED_FDS,
                clamped_maxclients = effective,
                "maxclients clamped to fit the open-file limit (RLIMIT_NOFILE): the requested \
                 maxclients plus reserved fds exceeds the file-descriptor budget; raise 'ulimit -n' \
                 or the systemd LimitNOFILE= to restore the requested ceiling"
            );
            cfg.maxclients = effective;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = cfg;
        tracing::debug!("RLIMIT_NOFILE budgeting is a no-op on this platform");
    }
}

/// Read the current `RLIMIT_NOFILE` soft/hard limits, or `None` if `getrlimit` fails.
// `rlim_t` is `u64` on Linux/macOS (where the `as u64` is a no-op clippy would flag) but
// is a signed 64-bit type on some BSDs, so the cast is a genuine portability conversion.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn current_nofile_limits() -> Option<(u64, u64)> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` writes the two-field `rlimit` we pass by pointer for the
    // valid resource id `RLIMIT_NOFILE`; it touches no other memory.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, std::ptr::from_mut(&mut limit)) };
    if rc == 0 {
        Some((limit.rlim_cur as u64, limit.rlim_max as u64))
    } else {
        None
    }
}

/// Raise the `RLIMIT_NOFILE` SOFT limit to `target` (leaving the hard limit at
/// `hard`, which a process may not raise without privilege). `target` must not exceed
/// `hard`. Returns the OS error on failure.
// See `current_nofile_limits`: the `as libc::rlim_t` cast is a portability conversion
// (a no-op on Linux/macOS, meaningful where `rlim_t` is signed).
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn set_soft_nofile(target: u64, hard: u64) -> std::io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: target as libc::rlim_t,
        rlim_max: hard as libc::rlim_t,
    };
    // SAFETY: `setrlimit` reads the `rlimit` we pass by pointer for the valid resource
    // id `RLIMIT_NOFILE`; it touches no other memory. Raising the soft limit up to the
    // unchanged hard limit needs no privilege.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, std::ptr::from_ref(&limit)) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::{FdBudget, RESERVED_FDS, compute_fd_budget};

    #[test]
    fn soft_limit_already_covers_requested_is_a_noop() {
        // Soft limit comfortably above requested + reserved: no raise, no clamp.
        let b = compute_fd_budget(65_535, 65_535, 10_000, RESERVED_FDS);
        assert_eq!(
            b,
            FdBudget {
                effective_maxclients: 10_000,
                raise_soft_to: None,
            }
        );
    }

    #[test]
    fn exact_fit_boundary_is_a_noop() {
        // soft == requested + reserved is the exact-fit boundary: still no raise/clamp.
        let requested = 1_000;
        let soft = requested + RESERVED_FDS;
        let b = compute_fd_budget(soft, soft, requested, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, requested);
        assert_eq!(b.raise_soft_to, None);
    }

    #[test]
    fn low_soft_but_high_hard_raises_to_fit_without_clamping() {
        // The common production case: soft 1024, hard huge. Raise the soft limit to
        // exactly requested + reserved; the requested ceiling is preserved.
        let requested = 10_000;
        let b = compute_fd_budget(1024, 1_048_576, requested, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, requested);
        assert_eq!(b.raise_soft_to, Some(requested + RESERVED_FDS));
        // The raise target must be a strict increase over the current soft limit.
        assert!(b.raise_soft_to.unwrap() > 1024);
    }

    #[test]
    fn hard_limit_below_required_clamps_to_hard_minus_reserved() {
        // Container pinned at 1024/1024: cannot fit 10000 clients. Raising the soft
        // limit is a no-op (soft == hard), and maxclients clamps to hard - reserved.
        let requested = 10_000;
        let hard = 1024;
        let b = compute_fd_budget(hard, hard, requested, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, hard - RESERVED_FDS);
        assert_eq!(
            b.raise_soft_to, None,
            "soft == hard leaves nothing to raise"
        );
        assert!(
            b.effective_maxclients < requested,
            "must clamp below requested"
        );
    }

    #[test]
    fn hard_below_required_but_above_soft_raises_then_clamps() {
        // soft 256, hard 1024, requested 10000: raise the soft limit to the hard limit
        // (a real increase), and STILL clamp maxclients to hard - reserved.
        let requested = 10_000;
        let b = compute_fd_budget(256, 1024, requested, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, 1024 - RESERVED_FDS);
        assert_eq!(b.raise_soft_to, Some(1024));
    }

    #[test]
    fn hard_limit_below_reserved_headroom_clamps_to_zero() {
        // Pathological: fewer fds than the reserved headroom. Clamp saturates to 0
        // (no client capacity) rather than underflowing.
        let b = compute_fd_budget(16, 16, 100, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, 0);
    }

    #[test]
    fn unlimited_maxclients_is_never_clamped_but_raises_headroom() {
        // requested == 0 disables the cap: never clamp. With soft < hard, raise the
        // soft limit to the hard limit to maximize the available fds.
        let b = compute_fd_budget(1024, 65_535, 0, RESERVED_FDS);
        assert_eq!(b.effective_maxclients, 0);
        assert_eq!(b.raise_soft_to, Some(65_535));
    }

    #[test]
    fn unlimited_maxclients_with_soft_equal_hard_does_not_raise() {
        // Unlimited, but no room to raise: no-op.
        let b = compute_fd_budget(65_535, 65_535, 0, RESERVED_FDS);
        assert_eq!(
            b,
            FdBudget {
                effective_maxclients: 0,
                raise_soft_to: None,
            }
        );
    }
}
