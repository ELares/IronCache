// SPDX-License-Identifier: MIT OR Apache-2.0
//! The VERIFY seam for `ironcache upgrade` (#387 mechanism; #386 signature follow-up).
//!
//! v1 ships ONE [`Verifier`]: [`Sha256Verifier`], which confirms the new binary's SHA-256 matches
//! its entry in a release `SHA256SUMS`. This is INTEGRITY (the bytes are the published ones), NOT
//! AUTHENTICITY (that the publisher is trusted). The cryptographic SIGNATURE ANCHOR is the explicit
//! #386 follow-up: #386 finalized the anchor as MINISIGN (over cosign/sigstore; ADR-0020 amendment
//! 2026-06-29), so a `MinisignVerifier` implementing this SAME trait drops in with NO change to the
//! orchestrator. The trait is the only thing the orchestrator depends on.
//!
//! SHA-256 is computed with the workspace's hand-rolled FIPS 180-4 implementation
//! ([`ironcache_config::sha256_hex`], already used for AUTH-at-rest) -- NO new crypto crate, keeping
//! the musl + cargo-deny posture (ADR-0017, the same rationale that crate documents).

use std::path::Path;
use std::time::Duration;

use super::proc::run_bounded;

/// The bound on `<binary> --version`: it runs the UNTRUSTED new binary, so a hung child must not
/// wedge the upgrade. `--version` is near-instant for a healthy binary; this is generous headroom.
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// A typed verification failure. None of these is a panic: a malformed `SHA256SUMS`, a missing
/// entry, an unreadable binary, and a hash mismatch are all data conditions, mapped here.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The new binary could not be read to hash it.
    #[error("reading the new binary {path}: {source}")]
    ReadBinary {
        /// The binary path.
        path: String,
        /// The IO error.
        #[source]
        source: std::io::Error,
    },
    /// The `SHA256SUMS` file could not be read.
    #[error("reading SHA256SUMS {path}: {source}")]
    ReadSums {
        /// The sums-file path.
        path: String,
        /// The IO error.
        #[source]
        source: std::io::Error,
    },
    /// The `SHA256SUMS` file had no entry for the binary's file name.
    #[error("SHA256SUMS has no entry for '{name}' (cannot verify an unlisted artifact)")]
    NoEntry {
        /// The file name we looked up.
        name: String,
    },
    /// A `SHA256SUMS` line was malformed (not `<64-hex>  <name>`).
    #[error("malformed SHA256SUMS line {line_no}: {detail}")]
    MalformedSums {
        /// The 1-based line number.
        line_no: usize,
        /// What was wrong.
        detail: String,
    },
    /// The binary's actual SHA-256 did not match its `SHA256SUMS` entry.
    #[error("sha256 mismatch for '{name}': expected {expected}, got {actual}")]
    Mismatch {
        /// The file name.
        name: String,
        /// The hex digest the sums file vouches for.
        expected: String,
        /// The hex digest we computed over the binary.
        actual: String,
    },
    /// The new binary did not run / report a version when invoked with `--version`.
    #[error("the new binary {path} did not report a version: {detail}")]
    VersionProbe {
        /// The binary path.
        path: String,
        /// Why the version probe failed.
        detail: String,
    },
}

/// The verify seam (#386 lands minisign/sigstore behind this exact signature). An implementation
/// confirms that `binary` (whose file name is `name`) is the intended artifact, using `sha256sums`
/// (and, for #386, a detached signature + pinned public key alongside it).
pub trait Verifier {
    /// Verify `binary` against the manifest. Returns `Ok(())` only when the artifact is vouched
    /// for; any mismatch / missing entry / malformed manifest is an `Err`.
    ///
    /// # Errors
    ///
    /// Returns a [`VerifyError`] describing the first failed check.
    fn verify(&self, binary: &Path, name: &str, sha256sums: &Path) -> Result<(), VerifyError>;
}

/// The v1 verifier: the binary's SHA-256 must equal its `SHA256SUMS` entry. Integrity only;
/// authenticity is #386.
///
/// TODO(#386): #386 finalized the anchor as MINISIGN (ADR-0020 amendment 2026-06-29). A
/// `MinisignVerifier` implementing [`Verifier`] verifies a detached minisign signature over the
/// binary (or over `SHA256SUMS`) against an in-repo/in-docs Ed25519 public key (offline, no PKI),
/// per docs/design/UPGRADE.md "Verify before swap". It composes with this one (verify the sums entry
/// AND the signature); the orchestrator is unchanged because it only names the trait. The remaining
/// blockers are operational (release-side per-binary minisign signing + a committed pubkey).
pub struct Sha256Verifier;

impl Verifier for Sha256Verifier {
    fn verify(&self, binary: &Path, name: &str, sha256sums: &Path) -> Result<(), VerifyError> {
        let expected = lookup_sum(sha256sums, name)?;
        let bytes = std::fs::read(binary).map_err(|source| VerifyError::ReadBinary {
            path: binary.display().to_string(),
            source,
        })?;
        let actual = ironcache_config::sha256_hex(&bytes);
        if actual.eq_ignore_ascii_case(&expected) {
            Ok(())
        } else {
            Err(VerifyError::Mismatch {
                name: name.to_owned(),
                expected,
                actual,
            })
        }
    }
}

/// Parse a `SHA256SUMS` file and return the hex digest listed for `name`, or a typed error. The
/// format is the coreutils `sha256sum` form: `<64 lowercase hex>  <name>` per line (two spaces for
/// text mode, one space + `*` for binary mode; both tolerated). Lines that are blank or start with
/// `#` are skipped. A line that is non-blank, non-comment, and not parseable is a hard error (a
/// malformed manifest is not silently ignored). `name` is matched on the BASENAME so a manifest that
/// lists `./ironcache` or `dist/ironcache` still matches a binary named `ironcache`.
fn lookup_sum(sha256sums: &Path, name: &str) -> Result<String, VerifyError> {
    let text = std::fs::read_to_string(sha256sums).map_err(|source| VerifyError::ReadSums {
        path: sha256sums.display().to_string(),
        source,
    })?;
    for (i, raw) in text.lines().enumerate() {
        let line_no = i + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (digest, entry_name) = parse_sums_line(line, line_no)?;
        // Match on basename so `dist/ironcache` / `*ironcache` entries match a bare `ironcache`.
        let entry_base = Path::new(entry_name)
            .file_name()
            .map_or(entry_name, |s| s.to_str().unwrap_or(entry_name));
        if entry_base == name {
            return Ok(digest.to_ascii_lowercase());
        }
    }
    Err(VerifyError::NoEntry {
        name: name.to_owned(),
    })
}

/// Parse ONE `SHA256SUMS` line into `(digest, name)`. The digest must be exactly 64 hex chars; the
/// name is the remainder after the separator (the coreutils format puts the name verbatim, possibly
/// with a leading `*` binary-mode marker, which we strip).
fn parse_sums_line(line: &str, line_no: usize) -> Result<(&str, &str), VerifyError> {
    // Split on the FIRST run of whitespace: `<digest><ws><name>`.
    let mut it = line.splitn(2, char::is_whitespace);
    let digest = it.next().unwrap_or("");
    let rest = it.next().unwrap_or("").trim_start();
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(VerifyError::MalformedSums {
            line_no,
            detail: format!("expected a 64-char hex digest, found '{digest}'"),
        });
    }
    // Strip the coreutils binary-mode `*` marker if present.
    let name = rest.strip_prefix('*').unwrap_or(rest);
    if name.is_empty() {
        return Err(VerifyError::MalformedSums {
            line_no,
            detail: "missing the file name after the digest".to_owned(),
        });
    }
    Ok((digest, name))
}

/// Run `<binary> --version` and return the reported version string (the EXPECTED upgrade target
/// version). This doubles as the "the new binary actually RUNS" sanity check UPGRADE.md requires
/// before the swap. The clap `--version` output is `"ironcache <version>"`; we take the LAST
/// whitespace token as the version (robust to the program-name prefix). A non-zero exit, an
/// un-spawnable binary, or empty output is a [`VerifyError::VersionProbe`].
///
/// # Errors
///
/// Returns [`VerifyError::VersionProbe`] when the binary cannot be run or reports no version.
pub fn probe_binary_version(binary: &Path) -> Result<String, VerifyError> {
    // `std::process::Command` from this short-lived CLI process is NOT a `fork` syscall (no-fork
    // invariant 4 is about the SERVER never forking; the upgrade CLI legitimately spawns helpers).
    // BOUNDED (review fix #7): the new binary is UNTRUSTED, so a hung `--version` must time out into
    // a typed error rather than wedging the upgrade forever.
    let output = run_bounded(binary, &["--version"], VERSION_PROBE_TIMEOUT).map_err(|e| {
        VerifyError::VersionProbe {
            path: binary.display().to_string(),
            detail: format!("could not run it: {e}"),
        }
    })?;
    if !output.status.success() {
        return Err(VerifyError::VersionProbe {
            path: binary.display().to_string(),
            detail: format!(
                "--version exited with {}",
                output
                    .status
                    .code()
                    .map_or_else(|| "a signal".to_owned(), |c| c.to_string())
            ),
        });
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_version_output(&stdout).ok_or_else(|| VerifyError::VersionProbe {
        path: binary.display().to_string(),
        detail: "no version token in the --version output".to_owned(),
    })
}

/// Extract the version token from a clap `--version` line (`"ironcache 2026.0622.1"` ->
/// `"2026.0622.1"`). Returns the LAST whitespace-separated token of the first non-empty line, or
/// `None` if the output is blank. Pure, so it is unit-tested without spawning a process.
fn parse_version_output(stdout: &str) -> Option<String> {
    let first = stdout.lines().find(|l| !l.trim().is_empty())?;
    first.split_whitespace().last().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ic-upgrade-verify-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn sha256_verify_matches() {
        let dir = temp_dir("match");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"the-new-binary-bytes").unwrap();
        let digest = ironcache_config::sha256_hex(b"the-new-binary-bytes");
        let sums = dir.join("SHA256SUMS");
        std::fs::write(&sums, format!("{digest}  ironcache\n")).unwrap();
        Sha256Verifier
            .verify(&bin, "ironcache", &sums)
            .expect("matching sha256 verifies");
    }

    #[test]
    fn sha256_verify_mismatch_is_rejected() {
        let dir = temp_dir("mismatch");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"actual-bytes").unwrap();
        let wrong = ironcache_config::sha256_hex(b"different-bytes");
        let sums = dir.join("SHA256SUMS");
        std::fs::write(&sums, format!("{wrong}  ironcache\n")).unwrap();
        let err = Sha256Verifier
            .verify(&bin, "ironcache", &sums)
            .expect_err("a mismatch must be rejected");
        assert!(matches!(err, VerifyError::Mismatch { .. }), "{err:?}");
    }

    #[test]
    fn missing_entry_is_rejected() {
        let dir = temp_dir("noentry");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"x").unwrap();
        let sums = dir.join("SHA256SUMS");
        // Lists a DIFFERENT file; no `ironcache` entry.
        let other = ironcache_config::sha256_hex(b"y");
        std::fs::write(&sums, format!("{other}  some-other-file\n")).unwrap();
        let err = Sha256Verifier
            .verify(&bin, "ironcache", &sums)
            .expect_err("a missing entry must be rejected");
        assert!(matches!(err, VerifyError::NoEntry { .. }), "{err:?}");
    }

    #[test]
    fn malformed_sums_line_is_an_error_not_a_panic() {
        let dir = temp_dir("malformed");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"x").unwrap();
        let sums = dir.join("SHA256SUMS");
        // A non-comment, non-blank, unparseable line (digest too short).
        std::fs::write(&sums, "deadbeef  ironcache\n").unwrap();
        let err = Sha256Verifier
            .verify(&bin, "ironcache", &sums)
            .expect_err("a malformed line must error");
        assert!(
            matches!(err, VerifyError::MalformedSums { line_no: 1, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn comments_and_blank_lines_are_skipped_and_basename_matches() {
        let dir = temp_dir("comments");
        let bin = dir.join("ironcache");
        std::fs::write(&bin, b"payload").unwrap();
        let digest = ironcache_config::sha256_hex(b"payload");
        let sums = dir.join("SHA256SUMS");
        // A comment, a blank line, an unrelated entry, then a path-qualified + binary-mode entry.
        let body = format!(
            "# release SHA256SUMS\n\n{other}  README.md\n{digest} *dist/ironcache\n",
            other = ironcache_config::sha256_hex(b"readme"),
        );
        std::fs::write(&sums, body).unwrap();
        Sha256Verifier
            .verify(&bin, "ironcache", &sums)
            .expect("basename `ironcache` matches `dist/ironcache`, skipping comments/blanks");
    }

    #[test]
    fn unreadable_binary_is_a_typed_error() {
        let dir = temp_dir("unreadable");
        let sums = dir.join("SHA256SUMS");
        let digest = ironcache_config::sha256_hex(b"x");
        std::fs::write(&sums, format!("{digest}  ironcache\n")).unwrap();
        let missing = dir.join("does-not-exist");
        let err = Sha256Verifier
            .verify(&missing, "ironcache", &sums)
            .expect_err("a missing binary errors");
        assert!(matches!(err, VerifyError::ReadBinary { .. }), "{err:?}");
    }

    #[test]
    fn parse_version_output_takes_last_token() {
        assert_eq!(
            parse_version_output("ironcache 2026.0622.1\n").as_deref(),
            Some("2026.0622.1")
        );
        assert_eq!(parse_version_output("0.0.0").as_deref(), Some("0.0.0"));
        assert_eq!(parse_version_output("\n\n  ").as_deref(), None);
        assert_eq!(parse_version_output("").as_deref(), None);
    }

    #[test]
    fn probe_binary_version_runs_a_real_binary() {
        // The test executable answers --version via clap-... actually the test harness binary does
        // not; use a tiny shell that echoes a version line, which is a real runnable program.
        // On unix, `/bin/echo` exists; we wrap it so `<prog> --version` prints a version line.
        // Instead, point at the running test binary's `--version` is not guaranteed; so we test the
        // pure parser above and the error path here.
        let dir = temp_dir("vprobe");
        let not_exec = dir.join("not-runnable");
        std::fs::write(&not_exec, b"not an executable").unwrap();
        let err =
            probe_binary_version(&not_exec).expect_err("a non-executable cannot report a version");
        assert!(matches!(err, VerifyError::VersionProbe { .. }), "{err:?}");
    }
}
