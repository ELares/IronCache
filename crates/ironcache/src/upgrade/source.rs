// SPDX-License-Identifier: MIT OR Apache-2.0
//! The BINARY-SOURCE seam for `ironcache upgrade`.
//!
//! v1 ships ONE [`BinarySource`]: [`LocalFile`] -- the operator supplies the new binary already on
//! disk (the `--binary <path>` flag). The orchestrator only depends on the trait, so the HTTPS
//! auto-fetch and the GitHub-latest resolver are clean follow-ups (see the TODOs below); they slot
//! in WITHOUT a heavy HTTP dependency entering the default mechanism (the musl + cargo-deny posture,
//! ADR-0017): a fetcher would download to a temp path and return the SAME [`ResolvedBinary`], so the
//! verify -> swap -> health-gate flow is unchanged.

use std::path::{Path, PathBuf};

/// A typed source failure (no stringly-typed errors). v1 only fails when the local `--binary` path
/// is missing / not a file.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The supplied local binary path does not exist or is not a regular file.
    #[error("the --binary path {path} is not a readable file: {detail}")]
    NotAFile {
        /// The path that was not usable.
        path: String,
        /// Why it was not usable.
        detail: String,
    },
    /// The path had no file-name component (e.g. it ended in `..`), so we cannot derive the
    /// `SHA256SUMS` lookup key.
    #[error("the --binary path {path} has no file name to match against SHA256SUMS")]
    NoFileName {
        /// The offending path.
        path: String,
    },
}

/// A resolved new-binary artifact: where it is on disk now, and the file NAME used to find its
/// `SHA256SUMS` entry. A future HTTPS/GitHub source returns the same shape (a temp download path +
/// the published asset name).
#[derive(Debug, Clone)]
pub struct ResolvedBinary {
    /// The on-disk path to the new binary bytes (the local file in v1; a temp download later).
    pub path: PathBuf,
    /// The file name the `SHA256SUMS` entry is keyed by (basename of `path` in v1).
    pub name: String,
}

/// The fetch seam. An implementation makes the new binary available on the local filesystem and
/// reports its `SHA256SUMS` lookup name.
pub trait BinarySource {
    /// Make the new binary available locally and report it.
    ///
    /// # Errors
    ///
    /// Returns a [`SourceError`] when the artifact cannot be made available.
    fn resolve(&self) -> Result<ResolvedBinary, SourceError>;
}

/// The v1 source: a local file the operator already placed on disk (`--binary <path>`). No network,
/// no new dependency.
///
/// The HTTPS AUTO-FETCH (#394) is implemented in [`super::fetch`], NOT as a `BinarySource` impl: the
/// release ships TARBALLS with a `SHA256SUMS` over those tarballs, so the fetch must download BOTH,
/// verify the tarball, and EXTRACT the binary -- a shape [`ResolvedBinary`] (a single pre-verified
/// binary path) does not carry. So the CLI (`resolve_upgrade_source` in main.rs) materializes the
/// extracted binary + a derived per-binary manifest locally, then runs the SAME [`LocalFile`] +
/// [`super::verify::Sha256Verifier`] flow, leaving the orchestrator untouched. This seam stays for a
/// future in-process fetcher (should the static/cargo-deny posture ever admit a public-root HTTPS
/// client); [`super::fetch`] instead reuses the SYSTEM `curl`/`tar` as bounded subprocesses (no new
/// dependency). With the minisign anchor (#386) the signature over `SHA256SUMS` gates the fetch too.
pub struct LocalFile {
    path: PathBuf,
}

impl LocalFile {
    /// Build a local-file source for `path` (the `--binary` flag).
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        LocalFile { path }
    }
}

impl BinarySource for LocalFile {
    fn resolve(&self) -> Result<ResolvedBinary, SourceError> {
        let meta = std::fs::metadata(&self.path).map_err(|e| SourceError::NotAFile {
            path: self.path.display().to_string(),
            detail: e.to_string(),
        })?;
        if !meta.is_file() {
            return Err(SourceError::NotAFile {
                path: self.path.display().to_string(),
                detail: "not a regular file".to_owned(),
            });
        }
        let name = file_name_of(&self.path).ok_or_else(|| SourceError::NoFileName {
            path: self.path.display().to_string(),
        })?;
        Ok(ResolvedBinary {
            path: self.path.clone(),
            name,
        })
    }
}

/// The basename of `path` as an owned `String`, or `None` if it has no file-name component.
fn file_name_of(path: &Path) -> Option<String> {
    path.file_name().and_then(|s| s.to_str()).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ic-upgrade-source-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn local_file_resolves_path_and_name() {
        let dir = temp_dir("ok");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"bytes").unwrap();
        let r = LocalFile::new(bin.clone()).resolve().expect("resolves");
        assert_eq!(r.path, bin);
        assert_eq!(r.name, "ironcache");
    }

    #[test]
    fn missing_file_is_a_typed_error() {
        let dir = temp_dir("missing");
        let err = LocalFile::new(dir.join("nope"))
            .resolve()
            .expect_err("a missing file errors");
        assert!(matches!(err, SourceError::NotAFile { .. }), "{err:?}");
    }

    #[test]
    fn a_directory_is_not_a_file() {
        let dir = temp_dir("isdir");
        let err = LocalFile::new(dir.clone())
            .resolve()
            .expect_err("a directory is not a binary");
        assert!(matches!(err, SourceError::NotAFile { .. }), "{err:?}");
    }
}
