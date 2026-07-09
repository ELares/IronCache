// SPDX-License-Identifier: MIT OR Apache-2.0
//! CPU affinity primitives for the dedicated persist thread (#589).
//!
//! The per-slot Arc-COW snapshot (#588) moved the O(N) encode+fsync of a save onto a dedicated
//! `ic-persist-<shard>` OS thread so the datapath stops paying it inline. That thread still RUNS
//! somewhere, though, and under the thread-per-core topology (ADR-0002) the datapath threads are
//! confined to a pinned cpuset (via `taskset`/cpuset at the process level; IronCache does NOT set
//! per-thread affinity itself). At `SHARDS=16` on 8 pinned cores the persist thread is a 17th
//! runnable thread landing on one of those same 8 serving cores, so its encode STEALS serving time
//! and stretches the during-save tail. These primitives let the binary pin the persist thread to a
//! DEDICATED core OFF the datapath set, so the encode no longer competes for a serving core.
//!
//! This is an OPS / SCHEDULING concern that lives entirely off the engine decision path: it changes
//! only WHICH core a thread runs on, never a stored value, a timer, an ordering, or any output. So it
//! does not touch the determinism seam (ADR-0003) and adds no OS clock / entropy read.
//!
//! ## Platform support
//!
//! CPU affinity is a Linux scheduling primitive (`sched_setaffinity(2)`). On Linux these functions
//! do the real work via `libc`; on every other platform they are a graceful no-op ([`pin_thread_to_cpus`]
//! returns `Ok(())` without pinning and [`current_thread_cpus`] returns an empty set), so the persist
//! thread simply runs unpinned exactly as it does today. [`AFFINITY_SUPPORTED`] lets a caller detect
//! the difference and warn once when a pin was requested on an unsupported host.

/// Whether per-thread CPU affinity pinning is supported on this build's target OS. `true` only on
/// Linux (where [`pin_thread_to_cpus`] calls `sched_setaffinity`); `false` elsewhere, where the pin
/// is a no-op. A caller reads this to warn once when the operator asked for a persist-core pin on a
/// platform that cannot honor it, instead of silently doing nothing.
pub const AFFINITY_SUPPORTED: bool = cfg!(target_os = "linux");

/// The set of logical CPU ids the CALLING thread is currently allowed to run on (its affinity mask).
///
/// On Linux this reads the live mask with `sched_getaffinity`; the returned ids are the CPUs
/// currently permitted (e.g. the `taskset`-confined datapath set). On any other platform, or if the
/// query fails, it returns an EMPTY vec, meaning "affinity is unknown / unsupported here" -- a caller
/// selecting an "auto" persist core then makes no pin (graceful).
#[must_use]
#[cfg(target_os = "linux")]
pub fn current_thread_cpus() -> Vec<usize> {
    // SAFETY: `set` is a stack-owned, zero-initialized `cpu_set_t` (a fixed-size bitmap POD, valid to
    // zero). `sched_getaffinity(0, size, &mut set)` targets the CURRENT thread (pid 0) and writes ONLY
    // into `set` (bounded by the passed `size` = its byte length), touching no other memory. On a
    // non-zero return we treat the mask as unknown and return empty WITHOUT reading `set`. `CPU_ISSET`
    // reads a single in-range bit of the owned `set`. No pointer escapes this call.
    unsafe {
        let mut set: libc::cpu_set_t = core::mem::zeroed();
        let rc = libc::sched_getaffinity(0, core::mem::size_of::<libc::cpu_set_t>(), &raw mut set);
        if rc != 0 {
            return Vec::new();
        }
        let mut cpus = Vec::new();
        for cpu in 0..(libc::CPU_SETSIZE as usize) {
            if libc::CPU_ISSET(cpu, &set) {
                cpus.push(cpu);
            }
        }
        cpus
    }
}

/// Non-Linux: affinity is unsupported, so the current mask is unknown (empty).
#[must_use]
#[cfg(not(target_os = "linux"))]
pub fn current_thread_cpus() -> Vec<usize> {
    Vec::new()
}

/// Pin the CALLING thread to EXACTLY the given set of logical CPU ids (its new affinity mask).
///
/// On Linux this is `sched_setaffinity` on the current thread (pid 0). Note that the target ids need
/// NOT be a subset of the thread's CURRENT mask: the kernel only requires them to be within the
/// process's cpuset-cgroup-allowed CPUs, so a persist thread CAN escape a `taskset`-confined datapath
/// mask onto a reserved core outside it (the intended "give the server one extra core" deployment).
/// An empty `cpus` is a no-op (leaves the thread unpinned). On any non-Linux platform this is a no-op
/// returning `Ok(())`.
///
/// # Errors
///
/// On Linux, returns the underlying `sched_setaffinity` error (e.g. a requested CPU that does not
/// exist or is outside the process's allowed cpuset). The caller treats an error as "run unpinned"
/// rather than fatal, so a misconfigured core id degrades gracefully instead of failing a save.
#[cfg(target_os = "linux")]
pub fn pin_thread_to_cpus(cpus: &[usize]) -> std::io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    // SAFETY: `set` is a stack-owned, zero-initialized `cpu_set_t`. `CPU_SET(cpu, &mut set)` sets one
    // in-range bit of the owned `set` (we skip any id >= CPU_SETSIZE, which the macro does not bound).
    // `sched_setaffinity(0, size, &set)` reads ONLY `set` (bounded by the passed `size`) and applies
    // it to the CURRENT thread (pid 0); it mutates no memory we own. On a non-zero return we surface
    // errno via `last_os_error`. No pointer escapes this call.
    unsafe {
        let mut set: libc::cpu_set_t = core::mem::zeroed();
        for &cpu in cpus {
            if cpu < (libc::CPU_SETSIZE as usize) {
                libc::CPU_SET(cpu, &mut set);
            }
        }
        let rc =
            libc::sched_setaffinity(0, core::mem::size_of::<libc::cpu_set_t>(), &raw const set);
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Non-Linux: pinning is unsupported, so this is a graceful no-op (the thread stays unpinned).
#[cfg(not(target_os = "linux"))]
#[allow(clippy::missing_errors_doc)] // Infallible no-op on non-Linux; the Linux cfg documents the error.
pub fn pin_thread_to_cpus(_cpus: &[usize]) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinning_empty_set_is_a_noop_ok() {
        // An empty selection never pins and never errors, on every platform.
        assert!(pin_thread_to_cpus(&[]).is_ok());
    }

    #[test]
    fn affinity_supported_matches_target_os() {
        assert_eq!(AFFINITY_SUPPORTED, cfg!(target_os = "linux"));
    }

    // On Linux (the Docker CI path + a Linux host), assert the pin ACTUALLY takes: pin a dedicated
    // std thread to CPU 0 and read its mask back. Runs on its own thread so the affinity change is
    // isolated to that thread and reverts when it joins (never perturbing the test runner or its
    // siblings). CPU 0 always exists and is in the default cpuset, so this is stable in CI.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_pin_sets_current_thread_affinity() {
        let handle = std::thread::spawn(|| {
            pin_thread_to_cpus(&[0]).expect("pinning to cpu 0 succeeds on Linux");
            current_thread_cpus()
        });
        let cpus = handle.join().expect("pinned thread joins");
        assert_eq!(cpus, vec![0], "the pinned thread's mask is exactly cpu 0");
    }
}
