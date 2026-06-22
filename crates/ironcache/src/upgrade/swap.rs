// SPDX-License-Identifier: MIT OR Apache-2.0
//! The ATOMIC binary swap + one-slot rollback for `ironcache upgrade` (docs/design/UPGRADE.md
//! "Atomic swap with one retained slot").
//!
//! The swap NEVER opens the live executable for write (that would be `ETXTBSY` on a running binary
//! on Linux), and `target` is NEVER momentarily absent (so a kill / crash / power-loss at any point
//! always leaves a runnable binary at the systemd `ExecStart` path -- a node can never be bricked
//! into "no ExecStart binary").
//!
//! ## The never-absent single-rename idiom
//!
//! The old two-rename sequence (`rename(target -> target.old)` then `rename(target.new -> target)`)
//! left `target` ABSENT in the window between the two renames; a crash there bricked the unit. We
//! instead:
//!
//! 1. Stage the new bytes to a sibling `<target>.new` on the SAME filesystem, fsync'd, mode 0755.
//! 2. While `target` STILL exists, create the rollback slot `<target>.old` by HARD-LINKING the
//!    current `target` inode into it (`fs::hard_link`), falling back to a byte copy when hard-linking
//!    is unavailable (EXDEV across a bind mount, EMLINK, or a filesystem without hard links). The
//!    prior inode now has two names: `target` and `target.old`.
//! 3. ONE atomic `rename(target.new -> target)`. `rename(2)` atomically REPLACES the destination, so
//!    `target` transitions directly from the old inode to the new one with no absent window; the old
//!    inode survives because `target.old` still names it.
//!
//! An interruption therefore always leaves `target` pointing at EITHER the old inode (before the
//! final rename) or the new one (after it), never absent and never torn. A cross-device staged
//! `.new` is still `EXDEV` on the final rename (it must be on the target's mount); we surface that as
//! a typed [`SwapError::CrossDevice`].
//!
//! ## Symlinks
//!
//! If `target` is a SYMLINK (a common packaging layout, e.g. `/usr/local/bin/ironcache ->
//! /opt/ironcache/bin/ironcache`), we REFUSE with [`SwapError::SymlinkTarget`] rather than silently
//! clobbering the link into a real file (which would also leave `.old` a dangling symlink). The
//! operator points `--target` at the real binary.
//!
//! Exactly ONE predecessor is kept (`<target>.old`); a second upgrade overwrites it. No versioned
//! archive accumulates (the Simple choice, ADR-0017). [`rollback`] restores `.old` onto `target`
//! while PRESERVING the `.old` slot (so the rollback contract -- ".old always holds last-known-good"
//! -- survives a rollback and a subsequent failed upgrade can still roll back).

use std::io::Write as _;
use std::path::{Path, PathBuf};

/// A typed swap/rollback failure. Every filesystem error is mapped here (no `unwrap`/`panic` on IO).
#[derive(Debug, thiserror::Error)]
pub enum SwapError {
    /// Staging the new bytes to `<target>.new` failed (read source / write temp / fsync).
    #[error("staging the new binary to {dest}: {detail}")]
    Stage {
        /// The `.new` path being staged.
        dest: String,
        /// What failed.
        detail: String,
    },
    /// Establishing the `<target>.old` rollback slot failed (both the hard-link AND the copy
    /// fallback failed).
    #[error("establishing the rollback slot {old} from {target}: {detail}")]
    RollbackSlot {
        /// The `.old` path.
        old: String,
        /// The current target being preserved.
        target: String,
        /// What failed.
        detail: String,
    },
    /// A rename crossed a filesystem boundary (`EXDEV`): the staged `.new` is not on the same mount
    /// as `target`, so an atomic rename is impossible.
    #[error(
        "cross-device rename ({from} -> {to}): the new binary must be staged on the SAME \
         filesystem as the target (atomic rename cannot cross mounts)"
    )]
    CrossDevice {
        /// The rename source.
        from: String,
        /// The rename destination.
        to: String,
    },
    /// A rename failed for a reason other than `EXDEV` (e.g. a permission error).
    #[error("rename ({from} -> {to}): {detail}")]
    Rename {
        /// The rename source.
        from: String,
        /// The rename destination.
        to: String,
        /// The OS error detail.
        detail: String,
    },
    /// Rollback was asked for but the `<target>.old` slot does not exist (nothing to restore).
    #[error("no rollback slot at {old}: there is no prior binary to restore")]
    NoRollbackSlot {
        /// The expected `.old` path.
        old: String,
    },
    /// The target path has no parent directory (cannot place the sibling `.new`/`.old` files).
    #[error("the target path {target} has no parent directory")]
    NoParent {
        /// The offending target path.
        target: String,
    },
    /// The target is a SYMLINK; we refuse to clobber it (it would become a real file and `.old`
    /// would be a dangling link). Point `--target` at the real binary the symlink resolves to.
    #[error(
        "the target {target} is a symlink; point --target at the real binary it resolves to \
         (refusing to clobber a symlink into a regular file)"
    )]
    SymlinkTarget {
        /// The symlink path.
        target: String,
    },
}

/// The sibling staging path `<target>.new`.
#[must_use]
pub fn new_path(target: &Path) -> PathBuf {
    sibling(target, "new")
}

/// The sibling rollback slot `<target>.old`.
#[must_use]
pub fn old_path(target: &Path) -> PathBuf {
    sibling(target, "old")
}

/// `<target>` with `ext` appended to its FILE NAME (so `/usr/local/bin/ironcache` ->
/// `/usr/local/bin/ironcache.new`). Appends rather than `with_extension` so a target that already
/// has a dot in its name is handled (we want `foo.bin.new`, not `foo.new`).
fn sibling(target: &Path, ext: &str) -> PathBuf {
    let mut name = target.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(ext);
    match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.join(name),
        _ => PathBuf::from(name),
    }
}

/// Perform the never-absent atomic swap: stage `src` into `<target>.new`, establish the `.old`
/// rollback slot from the current `target` (hard-link, copy fallback), then ONE atomic
/// `rename(target.new -> target)`. After a successful swap the prior binary is at `<target>.old`
/// (the one rollback slot) and the new one is at `target`, and `target` was never absent.
///
/// Refuses a symlink `target` ([`SwapError::SymlinkTarget`]).
///
/// # Errors
///
/// Returns a [`SwapError`] on any stage/slot/rename failure. The staging and slot steps run BEFORE
/// the only mutation of `target` (the final rename), so a failure in them leaves the original
/// `target` fully intact.
pub fn swap(src: &Path, target: &Path) -> Result<(), SwapError> {
    if target.parent().is_none() {
        return Err(SwapError::NoParent {
            target: target.display().to_string(),
        });
    }
    // Refuse a symlink target up front (symlink_metadata does NOT follow the link).
    if let Ok(meta) = std::fs::symlink_metadata(target) {
        if meta.file_type().is_symlink() {
            return Err(SwapError::SymlinkTarget {
                target: target.display().to_string(),
            });
        }
    }
    let new = new_path(target);
    let old = old_path(target);

    // 1. Stage the new bytes onto the SAME filesystem as the target, fsync'd, executable.
    stage(src, &new)?;

    // 2. While `target` STILL exists, establish the .old rollback slot from its inode (hard-link,
    // copy fallback). A fresh install (no existing target) has nothing to preserve -- skip; remove
    // any stale .old so it never points at a binary that is no longer `target`.
    if target.exists() {
        establish_rollback_slot(target, &old)?;
    } else {
        let _ = std::fs::remove_file(&old);
    }

    // 3. ONE atomic rename: target.new -> target. `rename(2)` atomically REPLACES the destination, so
    // `target` is NEVER absent; the prior inode survives via the .old hard-link. fsync the parent
    // dir afterward so the new dir entry is durable across a crash.
    rename(&new, target)?;
    fsync_parent_dir(target);
    Ok(())
}

/// Roll back to the retained predecessor and PRESERVE the slot: restore `.old` onto `target` (via a
/// fresh hard-link / copy that REPLACES `target` atomically), so after the rollback BOTH `target`
/// and `.old` name the last-known-good binary. This keeps the documented invariant (".old always
/// holds last-known-good") intact, so a subsequent failed upgrade can still roll back. A stray
/// `<target>.new` is best-effort removed.
///
/// The restore itself is never-absent: we write the good bytes to a temp sibling, fsync, then ONE
/// atomic `rename(temp -> target)`. The `.old` slot is untouched by that rename, so it remains.
///
/// # Errors
///
/// Returns [`SwapError::NoRollbackSlot`] if there is no `.old`, or a stage/rename error.
pub fn rollback(target: &Path) -> Result<(), SwapError> {
    let old = old_path(target);
    if !old.exists() {
        return Err(SwapError::NoRollbackSlot {
            old: old.display().to_string(),
        });
    }
    // Stage the good bytes from .old into a temp sibling (preserving .old, never touching target
    // yet), then atomically rename it onto target. target is never absent; .old survives.
    let restore = sibling(target, "restore");
    stage(&old, &restore)?;
    rename(&restore, target)?;
    fsync_parent_dir(target);
    // Tidy any leftover staged new binary.
    let _ = std::fs::remove_file(new_path(target));
    Ok(())
}

/// Establish `old` as the rollback slot for the current `target` inode: prefer a HARD LINK (so
/// `.old` is a second name for the exact same bytes with no copy), falling back to a byte copy when
/// hard-linking is unavailable (EXDEV / EMLINK / a filesystem without hard links). Any pre-existing
/// `.old` is removed first (a hard link / rename cannot overwrite). On the copy fallback the bytes
/// are fsync'd so `.old` is durable.
fn establish_rollback_slot(target: &Path, old: &Path) -> Result<(), SwapError> {
    // hard_link / the copy both need a clear destination.
    let _ = std::fs::remove_file(old);
    match std::fs::hard_link(target, old) {
        Ok(()) => Ok(()),
        Err(_link_err) => {
            // Fall back to a durable byte copy (covers EXDEV across a bind mount, EMLINK, or a
            // filesystem with no hard-link support). `stage` reads the source + fsyncs the copy.
            stage(target, old).map_err(|e| SwapError::RollbackSlot {
                old: old.display().to_string(),
                target: target.display().to_string(),
                detail: format!("hard-link failed and the copy fallback also failed: {e}"),
            })
        }
    }
}

/// Copy `src` to `dest`, fsync it (and its directory) so the bytes are durable before any rename,
/// and set mode 0755 on Unix. We copy-then-fsync (not a bare `fs::copy`) so a crash right after the
/// swap cannot leave a `.new` (or `.old`) whose contents are not yet on stable storage.
fn stage(src: &Path, dest: &Path) -> Result<(), SwapError> {
    let bytes = std::fs::read(src).map_err(|e| SwapError::Stage {
        dest: dest.display().to_string(),
        detail: format!("reading source {}: {e}", src.display()),
    })?;
    // Write + fsync the file.
    let mut f = std::fs::File::create(dest).map_err(|e| SwapError::Stage {
        dest: dest.display().to_string(),
        detail: format!("creating: {e}"),
    })?;
    f.write_all(&bytes).map_err(|e| SwapError::Stage {
        dest: dest.display().to_string(),
        detail: format!("writing: {e}"),
    })?;
    f.sync_all().map_err(|e| SwapError::Stage {
        dest: dest.display().to_string(),
        detail: format!("fsync: {e}"),
    })?;
    set_executable(dest)?;
    fsync_parent_dir(dest);
    Ok(())
}

/// Best-effort fsync of `path`'s parent directory so a newly-created / renamed dir entry is durable
/// across a crash. A failure to open/sync the dir is non-fatal (older kernels / odd filesystems),
/// so this never errors; durability is a defense-in-depth, not a correctness gate.
fn fsync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(dirf) = std::fs::File::open(parent) {
            let _ = dirf.sync_all();
        }
    }
}

/// Set mode 0755 on `path` (Unix). A no-op on non-Unix (the executable bit is not a concept there).
#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), SwapError> {
    use std::os::unix::fs::PermissionsExt as _;
    let meta = std::fs::metadata(path).map_err(|e| SwapError::Stage {
        dest: path.display().to_string(),
        detail: format!("stat for chmod: {e}"),
    })?;
    let mut perm = meta.permissions();
    perm.set_mode(0o755);
    std::fs::set_permissions(path, perm).map_err(|e| SwapError::Stage {
        dest: path.display().to_string(),
        detail: format!("chmod 0755: {e}"),
    })
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), SwapError> {
    Ok(())
}

/// `rename(from -> to)`, mapping `EXDEV` to the typed [`SwapError::CrossDevice`] and any other error
/// to [`SwapError::Rename`].
fn rename(from: &Path, to: &Path) -> Result<(), SwapError> {
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(e) => {
            // EXDEV (cross-device link) is the one error with a specific, actionable message.
            #[cfg(unix)]
            let is_exdev = e.raw_os_error() == Some(libc::EXDEV);
            #[cfg(not(unix))]
            let is_exdev = false;
            if is_exdev {
                Err(SwapError::CrossDevice {
                    from: from.display().to_string(),
                    to: to.display().to_string(),
                })
            } else {
                Err(SwapError::Rename {
                    from: from.display().to_string(),
                    to: to.display().to_string(),
                    detail: e.to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ic-upgrade-swap-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn swap_moves_new_into_place_keeping_old() {
        let dir = temp_dir("swap");
        let target = dir.join("ironcache");
        let src = dir.join("ironcache.candidate");
        std::fs::write(&target, b"OLD").unwrap();
        std::fs::write(&src, b"NEW").unwrap();

        swap(&src, &target).expect("swap succeeds");

        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"NEW",
            "target is the new binary"
        );
        assert_eq!(
            std::fs::read(old_path(&target)).unwrap(),
            b"OLD",
            "the .old slot holds the prior binary"
        );
        assert!(
            !new_path(&target).exists(),
            "the .new staging file was consumed by the rename"
        );
    }

    /// The CORE never-absent property (CRITICAL fix #1): `target` exists at EVERY observable step of
    /// the swap, and a failure injected BEFORE the final rename (here: a stage failure from a missing
    /// source) leaves the ORIGINAL target fully intact -- never absent, never torn.
    #[test]
    fn target_is_never_absent_across_swap() {
        let dir = temp_dir("neverabsent");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"ORIGINAL").unwrap();

        // A swap whose SOURCE is missing fails during staging (step 1), which is before any mutation
        // of target. The original target must be byte-for-byte intact and present.
        let missing = dir.join("does-not-exist");
        let err = swap(&missing, &target).expect_err("stage failure");
        assert!(matches!(err, SwapError::Stage { .. }), "{err:?}");
        assert!(
            target.exists(),
            "target must remain present after a pre-rename failure"
        );
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"ORIGINAL",
            "target bytes are intact after a pre-rename failure"
        );

        // A successful swap: at the end, target is present (the new bytes) and was never absent. We
        // cannot observe the intermediate instant from a single thread, but the IMPLEMENTATION only
        // ever rename(.new -> target) over an existing target (atomic replace), with .old already
        // hard-linked, so the inode behind `target` is always resolvable. Assert the post-state and
        // that .old preserved the prior bytes (proving the prior inode survived the replace).
        let src = dir.join("cand");
        std::fs::write(&src, b"NEW").unwrap();
        swap(&src, &target).expect("swap succeeds");
        assert!(target.exists(), "target present after a successful swap");
        assert_eq!(std::fs::read(&target).unwrap(), b"NEW");
        assert_eq!(
            std::fs::read(old_path(&target)).unwrap(),
            b"ORIGINAL",
            ".old holds the prior inode's bytes (it survived the atomic replace)"
        );
    }

    #[test]
    fn swap_then_rollback_restores_old_and_preserves_slot() {
        let dir = temp_dir("rollback");
        let target = dir.join("ironcache");
        let src = dir.join("cand");
        std::fs::write(&target, b"OLD").unwrap();
        std::fs::write(&src, b"NEW").unwrap();
        swap(&src, &target).expect("swap");
        assert_eq!(std::fs::read(&target).unwrap(), b"NEW");

        rollback(&target).expect("rollback succeeds");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"OLD",
            "rollback restored the prior binary"
        );
        // CRITICAL fix #5: the .old slot is PRESERVED across a rollback (still holds last-known-good).
        assert!(
            old_path(&target).exists(),
            "the .old slot survives a rollback (not consumed)"
        );
        assert_eq!(
            std::fs::read(old_path(&target)).unwrap(),
            b"OLD",
            ".old still holds the last-known-good binary after a rollback"
        );
        assert!(
            target.exists(),
            "target is present after a rollback (never absent)"
        );
    }

    /// After a rollback, a SECOND failed upgrade can STILL roll back (the slot was preserved, so we
    /// never hit NoRollbackSlot -> "manual intervention"). CRITICAL fix #5.
    #[test]
    fn rollback_is_repeatable_after_a_rollback() {
        let dir = temp_dir("repeat");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"GOOD").unwrap();
        let src = dir.join("bad");
        std::fs::write(&src, b"BAD").unwrap();
        swap(&src, &target).expect("swap to BAD");
        rollback(&target).expect("first rollback");
        assert_eq!(std::fs::read(&target).unwrap(), b"GOOD");
        // A second upgrade attempt + rollback still works (the slot is intact).
        let src2 = dir.join("bad2");
        std::fs::write(&src2, b"BAD2").unwrap();
        swap(&src2, &target).expect("swap to BAD2");
        assert_eq!(std::fs::read(&target).unwrap(), b"BAD2");
        rollback(&target).expect("second rollback still has a slot");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"GOOD",
            "second rollback restored GOOD"
        );
    }

    #[test]
    fn second_swap_overwrites_the_single_old_slot() {
        let dir = temp_dir("twoslots");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"V1").unwrap();
        let src2 = dir.join("v2");
        std::fs::write(&src2, b"V2").unwrap();
        swap(&src2, &target).expect("swap to v2");
        assert_eq!(std::fs::read(old_path(&target)).unwrap(), b"V1");
        let src3 = dir.join("v3");
        std::fs::write(&src3, b"V3").unwrap();
        swap(&src3, &target).expect("swap to v3");
        // Exactly one slot: .old now holds V2 (V1 is gone), not a versioned archive.
        assert_eq!(std::fs::read(&target).unwrap(), b"V3");
        assert_eq!(std::fs::read(old_path(&target)).unwrap(), b"V2");
    }

    #[test]
    fn fresh_install_with_no_existing_target_has_no_old_slot() {
        let dir = temp_dir("fresh");
        let target = dir.join("ironcache"); // does not exist yet
        let src = dir.join("cand");
        std::fs::write(&src, b"NEW").unwrap();
        swap(&src, &target).expect("fresh install swaps in");
        assert_eq!(std::fs::read(&target).unwrap(), b"NEW");
        assert!(
            !old_path(&target).exists(),
            "no prior binary -> no .old slot"
        );
    }

    /// A stale `.old` from a prior install is removed on a fresh install (no existing target), so
    /// `.old` never points at a binary that is no longer the predecessor.
    #[test]
    fn fresh_install_clears_a_stale_old_slot() {
        let dir = temp_dir("staleold");
        let target = dir.join("ironcache");
        std::fs::write(old_path(&target), b"STALE").unwrap(); // a leftover .old, no target
        let src = dir.join("cand");
        std::fs::write(&src, b"NEW").unwrap();
        swap(&src, &target).expect("fresh install");
        assert!(
            !old_path(&target).exists(),
            "the stale .old was cleared on the fresh install"
        );
    }

    #[test]
    fn rollback_without_a_slot_is_a_typed_error() {
        let dir = temp_dir("noslot");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"X").unwrap();
        let err = rollback(&target).expect_err("no .old -> error");
        assert!(matches!(err, SwapError::NoRollbackSlot { .. }), "{err:?}");
    }

    #[test]
    fn missing_source_is_a_typed_stage_error() {
        let dir = temp_dir("nosrc");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"OLD").unwrap();
        let err = swap(&dir.join("does-not-exist"), &target).expect_err("missing src errors");
        assert!(matches!(err, SwapError::Stage { .. }), "{err:?}");
        // The target is untouched on a stage failure (we stage BEFORE any mutation of target).
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"OLD",
            "target untouched on stage failure"
        );
    }

    /// CRITICAL fix #6: a SYMLINK target is refused with a typed error, NOT clobbered into a real
    /// file (which would also dangle .old).
    #[cfg(unix)]
    #[test]
    fn symlink_target_is_refused() {
        let dir = temp_dir("symlink");
        let real = dir.join("real-ironcache");
        std::fs::write(&real, b"REAL").unwrap();
        let link = dir.join("ironcache");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let src = dir.join("cand");
        std::fs::write(&src, b"NEW").unwrap();
        let err = swap(&src, &link).expect_err("a symlink target is refused");
        assert!(matches!(err, SwapError::SymlinkTarget { .. }), "{err:?}");
        // The symlink and its real target are untouched.
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read(&real).unwrap(),
            b"REAL",
            "the real binary is untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn staged_binary_is_executable() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir("exec");
        let target = dir.join("ironcache");
        let src = dir.join("cand");
        std::fs::write(&src, b"NEW").unwrap();
        // No existing target: fresh install.
        swap(&src, &target).expect("swap");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755, "the installed binary is mode 0755");
    }

    /// The copy-fallback path of `establish_rollback_slot` produces a correct `.old` even when a hard
    /// link cannot be made. We exercise the fallback directly (hard links are available in tmp, so we
    /// call the copy path via `stage` to prove the bytes + exec bit are right), and confirm the slot
    /// contents match the target.
    #[cfg(unix)]
    #[test]
    fn rollback_slot_copy_fallback_preserves_bytes() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir("copyfallback");
        let target = dir.join("ironcache");
        std::fs::write(&target, b"GOODBYTES").unwrap();
        let old = old_path(&target);
        // Drive the copy path (stage) directly, the same code the fallback uses.
        stage(&target, &old).expect("copy fallback stages the slot");
        assert_eq!(std::fs::read(&old).unwrap(), b"GOODBYTES");
        assert_eq!(
            std::fs::metadata(&old).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
