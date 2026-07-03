// SPDX-License-Identifier: MIT OR Apache-2.0
//! The upgrade-handoff snapshot STAGING target + its RAM-headroom guard (#390 Phase 2b).
//!
//! Phase 1's `ironcache upgrade` downtime is dominated by the snapshot save+load against the durable
//! `data_dir` (EBS gp3 on prod). For the HANDOFF snapshot ONLY, staging it on tmpfs (`/dev/shm`)
//! removes the disk I/O legs; the durable periodic snapshot in `data_dir` is unchanged.
//!
//! The hazard this module guards: tmpfs pages ARE RAM, so writing a handoff snapshot LARGER than the
//! free RAM would push the box into swap or OOM -- strictly worse than the disk it replaced, and
//! during a handoff the live dataset is still resident (a transient ~2x footprint). So the target is
//! chosen by a RAM-HEADROOM GUARD: tmpfs only when the snapshot plus headroom fits in the currently
//! available RAM, else the durable `data_dir`. The perf win (materially lower save+load on a multi-GB
//! set, WARM_RESTART.md's acceptance hook) is a pinned-host measurement; the OOM-prevention guard is
//! the correctness core, validated here.
//!
//! The DECISION ([`handoff_target`] + [`headroom_for`]) is pure + cfg-free (truth-table tested on
//! every host); the available-RAM read ([`available_ram_bytes`], `/proc/meminfo`) is Linux, with a
//! `None` fallback everywhere else that makes the guard take the safe `data_dir` path.

use std::path::{Path, PathBuf};

/// Where to stage the upgrade-handoff snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffTarget {
    /// tmpfs (RAM-backed, no disk I/O): the fast path, chosen ONLY when the snapshot fits in RAM
    /// with headroom. Carries the staging directory to write into.
    Tmpfs(PathBuf),
    /// The durable `data_dir` (disk): the SAFE fallback when tmpfs would risk OOM, when the RAM is
    /// unknown, or on a non-Linux host.
    DataDir,
}

/// The default tmpfs staging mount (Linux `/dev/shm`, a tmpfs on essentially every distro).
pub const DEFAULT_TMPFS_DIR: &str = "/dev/shm";

/// The minimum RAM headroom floor (512 MiB): even for a tiny snapshot, keep this much RAM free so
/// staging never starves the live process or the incoming new process.
const HEADROOM_FLOOR_BYTES: u64 = 512 * 1024 * 1024;

/// The RAM headroom to require ABOVE the snapshot before choosing tmpfs: `max(25% of the snapshot,
/// 512 MiB)`. The fraction scales the guard with the dataset (a big handoff needs proportionally more
/// slack for the transient 2x footprint); the floor covers small datasets.
#[must_use]
pub fn headroom_for(snapshot_bytes: u64) -> u64 {
    (snapshot_bytes / 4).max(HEADROOM_FLOOR_BYTES)
}

/// Choose the handoff staging target (#390): tmpfs ONLY when the estimated snapshot size plus
/// `headroom_bytes` fits in `available_ram_bytes`, else the durable `data_dir`. Pure + total.
///
/// - `snapshot_bytes`: the estimated on-tmpfs snapshot size (~ the resident dataset).
/// - `available_ram_bytes`: free RAM right now (`/proc/meminfo` `MemAvailable`); `None` => unknown =>
///   the safe `data_dir` path (never gamble on tmpfs without knowing the RAM).
/// - `headroom_bytes`: RAM that must remain free above the snapshot ([`headroom_for`]).
/// - `tmpfs_dir`: the tmpfs mount to stage under ([`DEFAULT_TMPFS_DIR`]).
#[must_use]
pub fn handoff_target(
    snapshot_bytes: u64,
    available_ram_bytes: Option<u64>,
    headroom_bytes: u64,
    tmpfs_dir: &Path,
) -> HandoffTarget {
    let Some(avail) = available_ram_bytes else {
        return HandoffTarget::DataDir; // unknown RAM -> do not gamble on tmpfs
    };
    match snapshot_bytes.checked_add(headroom_bytes) {
        // Fits with headroom -> stage on tmpfs, in a node-private subdir of the mount.
        Some(need) if need <= avail => HandoffTarget::Tmpfs(tmpfs_dir.join("ironcache-handoff")),
        // Does not fit (or the addition overflowed) -> the durable disk path, which does not eat RAM.
        _ => HandoffTarget::DataDir,
    }
}

/// The currently-available RAM in bytes, from `/proc/meminfo` `MemAvailable` (Linux). `None` on any
/// non-Linux host or an unreadable/unparseable file, which makes [`handoff_target`] take the safe
/// `data_dir` path.
#[must_use]
pub fn available_ram_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        for line in meminfo.lines() {
            // Format: "MemAvailable:   12345678 kB".
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
                return kb.checked_mul(1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux: tmpfs handoff staging is a Linux prod optimization; the guard self-binds to
        // the data_dir path here.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_TMPFS_DIR, HEADROOM_FLOOR_BYTES, HandoffTarget, available_ram_bytes,
        handoff_target, headroom_for,
    };
    use std::path::Path;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn handoff_target_picks_tmpfs_only_when_it_fits_with_headroom() {
        let tmpfs = Path::new(DEFAULT_TMPFS_DIR);

        // 1 GiB snapshot, 8 GiB free, 512 MiB headroom -> fits -> tmpfs (in the node subdir).
        assert_eq!(
            handoff_target(GIB, Some(8 * GIB), HEADROOM_FLOOR_BYTES, tmpfs),
            HandoffTarget::Tmpfs(tmpfs.join("ironcache-handoff"))
        );

        // 6 GiB snapshot, 6 GiB free -> snapshot alone fits but NOT with headroom -> data_dir.
        assert_eq!(
            handoff_target(6 * GIB, Some(6 * GIB), HEADROOM_FLOOR_BYTES, tmpfs),
            HandoffTarget::DataDir
        );

        // Snapshot larger than free RAM -> data_dir (would OOM on tmpfs).
        assert_eq!(
            handoff_target(10 * GIB, Some(4 * GIB), HEADROOM_FLOOR_BYTES, tmpfs),
            HandoffTarget::DataDir
        );

        // Unknown RAM -> never gamble -> data_dir.
        assert_eq!(
            handoff_target(GIB, None, HEADROOM_FLOOR_BYTES, tmpfs),
            HandoffTarget::DataDir
        );

        // Exact fit (need == avail) is allowed (<=): 1 GiB + 512 MiB headroom == 1.5 GiB free.
        assert_eq!(
            handoff_target(
                GIB,
                Some(GIB + HEADROOM_FLOOR_BYTES),
                HEADROOM_FLOOR_BYTES,
                tmpfs
            ),
            HandoffTarget::Tmpfs(tmpfs.join("ironcache-handoff"))
        );

        // Overflow-safe: a colossal snapshot + headroom cannot wrap to a small "fits" value.
        assert_eq!(
            handoff_target(u64::MAX, Some(u64::MAX), HEADROOM_FLOOR_BYTES, tmpfs),
            HandoffTarget::DataDir
        );
    }

    #[test]
    fn headroom_scales_with_the_snapshot_above_a_floor() {
        // Small snapshot -> the floor.
        assert_eq!(headroom_for(0), HEADROOM_FLOOR_BYTES);
        assert_eq!(headroom_for(GIB), HEADROOM_FLOOR_BYTES); // 256 MiB < 512 MiB floor
        // Large snapshot -> 25% of it (above the floor).
        assert_eq!(headroom_for(40 * GIB), 10 * GIB);
    }

    #[test]
    fn available_ram_is_some_on_linux_none_elsewhere() {
        let ram = available_ram_bytes();
        if cfg!(target_os = "linux") {
            // On a real Linux host (the CI io_uring runner + the local container) MemAvailable is a
            // sane positive number.
            let bytes = ram.expect("linux /proc/meminfo MemAvailable is readable");
            assert!(bytes > 0, "MemAvailable should be a positive byte count");
        } else {
            assert!(
                ram.is_none(),
                "non-Linux returns None (guard falls back to data_dir)"
            );
        }
    }
}
