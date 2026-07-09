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
//!
//! ## The headroom is load-safe because the OLD process exits before the NEW one loads
//!
//! `MemAvailable` already EXCLUDES the resident live dataset (`x`), so `snapshot + headroom <=
//! MemAvailable` bounds the tmpfs snapshot's INCREMENTAL cost at SAVE time. The wired load path
//! (`persist::PersistState` -> `coordinator::load_shard_on_boot` -> `ironcache_persist`) does NOT
//! mmap-reattach; it DESERIALIZES the tmpfs snapshot into a second in-heap copy, so the load-time
//! peak is `tmpfs(x) + heap(x)`. That still fits under this guard because the `ironcache upgrade`
//! flow RESTARTS the process: the OLD process (holding heap `x`) has EXITED before the NEW process
//! loads its heap `x`, so the old heap is freed while only the tmpfs pages persist. With
//! `MemAvailable_save ~= Total - x(old heap) - Other` and the guard admitting only
//! `x + 0.25x <= MemAvailable_save` (i.e. `2.25x + Other <= Total`), the load-time need
//! `tmpfs(x) + heap(x) + Other = 2x + Other` is `< 2.25x + Other <= Total` -- it fits with margin.
//! After the load+swap the tmpfs handoff is CLEANED UP (freeing `x`), so the 2x is only transient.
//! The estimate the caller feeds is the WHOLE-process allocation (`>= x`), a further conservative
//! margin.

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

/// The fixed staging SUBDIR name under the tmpfs base. NODE-LOCAL + well-known so the saving (old)
/// and loading (new) process rendezvous on it across a handoff; it is truncated/recreated per
/// handoff save and REMOVED after a successful load-on-boot (never leaked across upgrades).
pub const HANDOFF_SUBDIR: &str = "ironcache-handoff";

/// The staging directory the handoff snapshot files (`dump-shard-<n>.icss` + `dump.manifest`) are
/// written into, under the tmpfs `base`. The single source of truth both the save-side target and
/// the load-on-boot resolver derive, so they can never disagree on the path.
#[must_use]
pub fn handoff_staging_dir(base: &Path) -> PathBuf {
    base.join(HANDOFF_SUBDIR)
}

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
        // Fits with headroom -> stage on tmpfs, at a FIXED well-known name: the old + new process
        // rendezvous on it across the handoff, so it must NOT be pid-scoped. Node-LOCAL, not
        // instance-private -- two instances on one host (or a stale dir from a crashed prior handoff)
        // would collide; out of scope for #390's single-node handoff (the caller truncates/creates it).
        Some(need) if need <= avail => HandoffTarget::Tmpfs(handoff_staging_dir(tmpfs_dir)),
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

/// Resolve the tmpfs BASE to stage the handoff under, or `None` when tmpfs staging is unavailable
/// -- a non-Linux host, or a base that is NOT on a tmpfs/ramfs mount (a regular disk dir the
/// operator mislabeled, which would keep the disk I/O legs and could fill the disk). `None` makes
/// the caller stage on the durable `data_dir` (a warning, NOT a failure -- the #390 non-Linux /
/// no-tmpfs fallback). `configured` is the operator's `upgrade_handoff_dir` (`None` => the built-in
/// [`DEFAULT_TMPFS_DIR`]).
///
/// The base itself may not exist yet (the save side creates it lazily): the tmpfs test is a
/// LEXICAL mount-prefix match against `/proc/mounts`, so a not-yet-created subdir of a tmpfs mount
/// still qualifies (it will be created ON that mount). This uses no `unsafe` FFI (the crate is
/// `#![forbid(unsafe_code)]`); the RAM-headroom guard ([`handoff_target`] + `MemAvailable`) is the
/// OOM protection, and a too-small tmpfs (ENOSPC on write) degrades cleanly via the save fallback.
#[must_use]
pub fn usable_tmpfs_base(configured: Option<&Path>) -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let base = configured.map_or_else(|| PathBuf::from(DEFAULT_TMPFS_DIR), Path::to_path_buf);
        let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
        if path_is_on_tmpfs(&base, &mounts) {
            Some(base)
        } else {
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = configured;
        None
    }
}

/// Whether `path` lands on a tmpfs / ramfs (RAM-backed) mount, per the `/proc/mounts` text `mounts`.
/// Pure + total (no I/O): the LONGEST mount point that is a path-prefix of `path` wins, and we
/// report whether that mount's fstype is `tmpfs` or `ramfs`. Split out so it is unit-testable with a
/// synthetic mount table.
#[cfg(target_os = "linux")]
fn path_is_on_tmpfs(path: &Path, mounts: &str) -> bool {
    let target = path.to_string_lossy();
    let mut best_len = 0usize;
    let mut best_is_tmpfs = false;
    for line in mounts.lines() {
        // `/proc/mounts` columns: <dev> <mountpoint> <fstype> <opts> <dump> <pass>. The mountpoint
        // may octal-escape spaces (`\040`) etc; unescape it before the prefix test.
        let mut cols = line.split_whitespace();
        let (Some(_dev), Some(mp_raw), Some(fstype)) = (cols.next(), cols.next(), cols.next())
        else {
            continue;
        };
        let mp = unescape_mount_field(mp_raw);
        if is_path_prefix(&mp, &target) && mp.len() >= best_len {
            best_len = mp.len();
            best_is_tmpfs = fstype == "tmpfs" || fstype == "ramfs";
        }
    }
    best_is_tmpfs
}

/// Whether `mount_point` is a PATH prefix of `target` (equal, or an ancestor directory). `/` is a
/// prefix of everything; otherwise `target` must equal `mount_point` or start with `mount_point/`.
#[cfg(target_os = "linux")]
fn is_path_prefix(mount_point: &str, target: &str) -> bool {
    if mount_point == "/" {
        return true;
    }
    target == mount_point
        || target
            .strip_prefix(mount_point)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Unescape the octal `\NNN` sequences `/proc/mounts` uses for spaces/tabs/newlines/backslashes in a
/// mount-point field, so the prefix test compares real path bytes.
#[cfg(target_os = "linux")]
fn unescape_mount_field(field: &str) -> String {
    if !field.contains('\\') {
        return field.to_owned();
    }
    let bytes = field.as_bytes();
    let mut out = String::with_capacity(field.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let oct = &field[i + 1..i + 4];
            if let Ok(code) = u8::from_str_radix(oct, 8) {
                out.push(code as char);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_TMPFS_DIR, HANDOFF_SUBDIR, HEADROOM_FLOOR_BYTES, HandoffTarget,
        available_ram_bytes, handoff_staging_dir, handoff_target, headroom_for, usable_tmpfs_base,
    };
    use std::path::Path;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn staging_dir_is_the_subdir_under_the_base() {
        // Both the save target and the load resolver derive the SAME path from this helper.
        assert_eq!(
            handoff_staging_dir(Path::new("/dev/shm")),
            Path::new("/dev/shm").join(HANDOFF_SUBDIR)
        );
        assert!(
            handoff_target(
                GIB,
                Some(8 * GIB),
                HEADROOM_FLOOR_BYTES,
                Path::new(DEFAULT_TMPFS_DIR)
            ) == HandoffTarget::Tmpfs(handoff_staging_dir(Path::new(DEFAULT_TMPFS_DIR)))
        );
    }

    #[test]
    fn usable_tmpfs_base_accepts_dev_shm_on_linux() {
        if cfg!(target_os = "linux") && Path::new(DEFAULT_TMPFS_DIR).exists() {
            // /dev/shm is a tmpfs on the CI Linux runner + the local container -> Some.
            assert_eq!(
                usable_tmpfs_base(None).as_deref(),
                Some(Path::new(DEFAULT_TMPFS_DIR)),
                "the default /dev/shm resolves to a usable tmpfs base on Linux"
            );
            // A path whose every ancestor is a non-existent nonsense root cannot be tmpfs -> None,
            // proving the resolver falls back to data_dir when the base is not a RAM mount.
            assert_eq!(
                usable_tmpfs_base(Some(Path::new("/ic-no-such-root-9c3/handoff"))),
                None,
                "an unmounted / non-tmpfs base is rejected (data_dir fallback)"
            );
        } else {
            // Non-Linux (or no /dev/shm): tmpfs staging is unavailable -> None -> data_dir fallback.
            assert_eq!(usable_tmpfs_base(None), None);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn path_is_on_tmpfs_uses_the_longest_matching_mount() {
        use super::path_is_on_tmpfs;
        // A synthetic /proc/mounts: / is ext4, /dev is devtmpfs, /dev/shm is tmpfs, /mnt/ram is ramfs.
        let mounts = "\
/dev/sda1 / ext4 rw 0 0
devtmpfs /dev devtmpfs rw 0 0
tmpfs /dev/shm tmpfs rw 0 0
ramdisk /mnt/ram ramfs rw 0 0
/dev/sdb1 /data ext4 rw 0 0
";
        // /dev/shm + a not-yet-created subdir of it resolve to the tmpfs mount (longest prefix wins
        // over /dev devtmpfs and / ext4).
        assert!(path_is_on_tmpfs(Path::new("/dev/shm"), mounts));
        assert!(path_is_on_tmpfs(
            Path::new("/dev/shm/ironcache-handoff"),
            mounts
        ));
        // ramfs also counts as RAM-backed.
        assert!(path_is_on_tmpfs(Path::new("/mnt/ram/x"), mounts));
        // A disk mount + a bare root path are NOT tmpfs (data_dir fallback).
        assert!(!path_is_on_tmpfs(Path::new("/data/snap"), mounts));
        assert!(!path_is_on_tmpfs(Path::new("/var/lib/ironcache"), mounts));
        // A sibling of /dev/shm that only shares the /dev prefix is devtmpfs, not tmpfs.
        assert!(!path_is_on_tmpfs(Path::new("/dev/shmm"), mounts));
    }

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
