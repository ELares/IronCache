// SPDX-License-Identifier: MIT OR Apache-2.0
//! Persist-thread CPU pinning glue (#589): apply the `persist-cpu` knob to the CURRENT thread.
//!
//! This is the SAFE orchestration layer that ties the two seams together (the binary is
//! `#![forbid(unsafe_code)]`, so the syscall itself lives in `ironcache-runtime`):
//!
//! 1. parse the knob string into a [`PersistCpu`] policy ([`ironcache_config::parse_persist_cpu`]),
//! 2. read the CPUs this thread may run on ([`ironcache_runtime::current_thread_cpus`]),
//! 3. select the cpu(s) to pin to ([`ironcache_config::select_persist_cpus`]),
//! 4. pin via `sched_setaffinity` ([`ironcache_runtime::pin_thread_to_cpus`]).
//!
//! [`apply_persist_pin`] is called at the TOP of each `ic-persist-<shard>` thread closure
//! (coordinator.rs), so a save's off-core encode runs on the dedicated persist core rather than
//! stealing a pinned datapath serving core. It is a graceful no-op when the knob is unset (the
//! default), on non-Linux, or when the kernel rejects the requested core -- in every such case the
//! thread simply runs unpinned, exactly as it does today. It is purely a scheduling action off the
//! engine decision path, so ADR-0003 determinism is untouched (no clock, no entropy, no output
//! change). Diagnostics are announced ONCE (a process-global latch) to avoid per-save log spam.

use std::sync::atomic::{AtomicBool, Ordering};

/// Latches the one-time diagnostic so the per-save pin does not spam the log.
static ANNOUNCED: AtomicBool = AtomicBool::new(false);

/// Run `emit` at most once for the process (the first `apply_persist_pin` outcome that reaches it).
fn announce_once(emit: impl FnOnce()) {
    if ANNOUNCED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        emit();
    }
}

/// Pin the CALLING thread according to the resolved `persist-cpu` knob (`spec_raw`, the boot
/// [`ironcache_config::Config::persist_cpu`] value). Returns the cpu ids actually pinned (empty when
/// nothing was pinned), so a test can assert the effect; the persist thread ignores the return.
///
/// The knob was already validated at boot ([`ironcache_config::Config::validate`]); a parse error
/// here is therefore not expected, but is handled by warning once and leaving the thread unpinned.
pub fn apply_persist_pin(spec_raw: &str) -> Vec<usize> {
    let policy = match ironcache_config::parse_persist_cpu(spec_raw) {
        Ok(p) => p,
        Err(reason) => {
            announce_once(|| {
                tracing::warn!(
                    "persist-cpu '{spec_raw}' is invalid ({reason}); persist thread left unpinned"
                );
            });
            return Vec::new();
        }
    };
    // The default (and any explicit disable): no pin, no syscall, byte-unchanged behavior.
    if matches!(policy, ironcache_config::PersistCpu::Off) {
        return Vec::new();
    }
    // The operator asked for a pin but this platform cannot honor one: warn once, run unpinned.
    if !ironcache_runtime::AFFINITY_SUPPORTED {
        announce_once(|| {
            tracing::warn!(
                "persist-cpu is set ('{spec_raw}') but CPU affinity pinning is only supported on \
                 Linux; the persist thread runs unpinned on this platform"
            );
        });
        return Vec::new();
    }
    let online = ironcache_runtime::current_thread_cpus();
    let cpus = ironcache_config::select_persist_cpus(&policy, &online);
    if cpus.is_empty() {
        announce_once(|| {
            tracing::warn!(
                "persist-cpu '{spec_raw}' resolved to no cpu (empty selection); persist thread left \
                 unpinned"
            );
        });
        return Vec::new();
    }
    match ironcache_runtime::pin_thread_to_cpus(&cpus) {
        Ok(()) => {
            announce_once(|| {
                tracing::info!(
                    "persist thread pinned to cpu(s) {cpus:?} (#589 dedicated persist core; encode \
                     runs off the datapath cores)"
                );
            });
            cpus
        }
        Err(e) => {
            announce_once(|| {
                tracing::warn!(
                    "failed to pin persist thread to cpu(s) {cpus:?}: {e}; running unpinned (is the \
                     core within the process cpuset?)"
                );
            });
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_and_off_never_pin() {
        assert_eq!(apply_persist_pin(""), Vec::<usize>::new());
        assert_eq!(apply_persist_pin("off"), Vec::<usize>::new());
    }

    #[test]
    fn invalid_spec_is_a_graceful_noop() {
        // A malformed value (validation is upstream at boot) never panics; it runs unpinned.
        assert_eq!(apply_persist_pin("not-a-cpu"), Vec::<usize>::new());
    }

    // On Linux (Docker CI + a Linux host), an explicit core pins the CURRENT thread to exactly it.
    // Run on a dedicated std thread so the affinity change is isolated + reverts on join.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_explicit_core_pins_the_persist_thread() {
        let handle = std::thread::spawn(|| {
            let pinned = apply_persist_pin("0");
            (pinned, ironcache_runtime::current_thread_cpus())
        });
        let (pinned, mask) = handle.join().expect("pinned thread joins");
        assert_eq!(pinned, vec![0], "apply_persist_pin reports cpu 0");
        assert_eq!(
            mask,
            vec![0],
            "the thread's live affinity mask is exactly cpu 0"
        );
    }

    // On a non-Linux host, a set knob is a graceful no-op (no pin, no crash).
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_set_knob_is_a_noop() {
        assert_eq!(apply_persist_pin("0"), Vec::<usize>::new());
        assert_eq!(apply_persist_pin("auto"), Vec::<usize>::new());
    }
}
