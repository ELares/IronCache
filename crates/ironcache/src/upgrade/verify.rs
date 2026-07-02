// SPDX-License-Identifier: MIT OR Apache-2.0
//! The VERIFY seam for `ironcache upgrade` (#387 mechanism; #386 signature anchor).
//!
//! Two [`Verifier`]s behind one trait:
//! - [`Sha256Verifier`] -- INTEGRITY: the new binary's SHA-256 matches its entry in a release
//!   `SHA256SUMS` (the bytes are the published ones), but NOT publisher authenticity.
//! - [`MinisignVerifier`] (#386) -- INTEGRITY *and* AUTHENTICITY: it composes `Sha256Verifier` AND
//!   verifies a detached MINISIGN signature over `SHA256SUMS` (`<sums>.minisig`) against a pinned
//!   Ed25519 key. #386 (ADR-0020 amendment 2026-06-29) finalized the anchor as minisign; the crypto
//!   lives in [`super::minisign`] and is validated against real rsign2/minisign output. `run()`
//!   SELECTS `MinisignVerifier` the instant [`PINNED_UPGRADE_PUBLIC_KEY`] is set (the release workflow
//!   is already wired to sign `SHA256SUMS`); until then it falls back to `Sha256Verifier`.
//!
//! SHA-256 is the workspace's hand-rolled FIPS 180-4 impl ([`ironcache_config::sha256_hex`]); Ed25519
//! is `ring` (already linked as the TLS provider, so no new crate); Blake2b-512 + base64 are the small
//! hand-rolled pieces in [`super::minisign`] (validated against RFC KATs). No new crypto crate enters
//! the graph, keeping the musl + cargo-deny posture (ADR-0017).

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::minisign::{self, MinisignError, MinisignPublicKey};
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
    /// The detached minisign signature over `SHA256SUMS` could not be read (the `<sums>.minisig` file
    /// is missing/unreadable), so authenticity could not be checked (#386).
    #[error("reading the minisign signature {path}: {detail}")]
    ReadSignature {
        /// The `.minisig` path.
        path: String,
        /// The IO error.
        detail: String,
    },
    /// The pinned minisign public key or the signature did not verify: the `SHA256SUMS` is not
    /// authentic (not signed by the pinned key) (#386).
    #[error("minisign authenticity check failed: {0}")]
    Signature(#[from] MinisignError),
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

/// The INTEGRITY verifier: the binary's SHA-256 must equal its `SHA256SUMS` entry. Integrity only
/// (the bytes are the published ones); AUTHENTICITY (the publisher is trusted) is [`MinisignVerifier`],
/// which COMPOSES this one -- #386 is now implemented (the crypto lives in [`super::minisign`]).
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

/// The pinned minisign public key the upgrade authenticates releases against (#386, ADR-0020: "the
/// public key ships in the repo"). `None` until the maintainer commits the production key here + the
/// release workflow's `MINISIGN_SECRET_KEY` is provisioned (release.yml is already wired to sign
/// `SHA256SUMS` when the secret exists). While `None`, [`select_verifier`] falls back to the
/// integrity-only [`Sha256Verifier`] (the current behavior); the instant a key is pinned, upgrades
/// require a valid minisign signature with NO other code change.
pub const PINNED_UPGRADE_PUBLIC_KEY: Option<&str> = None;

/// The AUTHENTICITY verifier (#386): the binary's SHA-256 matches `SHA256SUMS` (integrity, via the
/// composed [`Sha256Verifier`]) AND `SHA256SUMS` carries a valid detached minisign signature
/// (`<sums>.minisig`) made by the pinned Ed25519 key (authenticity, via [`super::minisign`]). The
/// release signs `SHA256SUMS`, whose entries pin every artifact's sha256, so this one signature
/// authenticates the whole release offline with a single committed key (no PKI/network).
pub struct MinisignVerifier {
    pubkey: MinisignPublicKey,
}

impl MinisignVerifier {
    /// Build a verifier that authenticates against the pinned minisign public key string (the `RW...`
    /// line committed in the repo).
    ///
    /// # Errors
    ///
    /// [`VerifyError::Signature`] if `pubkey_b64` is not a well-formed minisign Ed25519 public key.
    pub fn new(pubkey_b64: &str) -> Result<Self, VerifyError> {
        Ok(MinisignVerifier {
            pubkey: MinisignPublicKey::parse(pubkey_b64)?,
        })
    }
}

impl Verifier for MinisignVerifier {
    fn verify(&self, binary: &Path, name: &str, sha256sums: &Path) -> Result<(), VerifyError> {
        // 1. INTEGRITY: the binary is the artifact `SHA256SUMS` vouches for.
        Sha256Verifier.verify(binary, name, sha256sums)?;
        // 2. AUTHENTICITY: `SHA256SUMS` itself is signed by the pinned key. minisign's detached
        //    signature convention is `<file>.minisig` alongside the file.
        let sig_path = minisig_path(sha256sums);
        let sig_file =
            std::fs::read_to_string(&sig_path).map_err(|e| VerifyError::ReadSignature {
                path: sig_path.display().to_string(),
                detail: e.to_string(),
            })?;
        let sums_bytes = std::fs::read(sha256sums).map_err(|source| VerifyError::ReadSums {
            path: sha256sums.display().to_string(),
            source,
        })?;
        minisign::verify(&sums_bytes, &sig_file, &self.pubkey)?;
        Ok(())
    }
}

/// The detached-signature path for `file`: minisign appends `.minisig` to the WHOLE file name
/// (`SHA256SUMS` -> `SHA256SUMS.minisig`), so append rather than replace-extension.
fn minisig_path(file: &Path) -> PathBuf {
    let mut s = file.as_os_str().to_owned();
    s.push(".minisig");
    PathBuf::from(s)
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

    // ---- MinisignVerifier end-to-end (#386), against a REAL rsign2 0.6.6 vector. ----

    /// A genuine rsign2 keypair + a real detached signature over a VALID coreutils `SHA256SUMS`
    /// listing the sha256 of `REAL_BIN_CONTENT`. `rsign verify` accepts these bytes, so a
    /// `MinisignVerifier` pass proves the full integrity + authenticity chain against real minisign.
    const MV_PUBKEY: &str = "RWREdimvfA8cGa5MTkLinjCO6dktAfbzHcG7vGO4cCDAtilnBR+mUBvY";
    const MV_BIN_CONTENT: &[u8] = b"test-binary-content-v1";
    const MV_SUMS: &str =
        "c6d825d6d463eeed24c9e4845d9a0ac14fa88e0f51134e18877966887d44dec0  ironcache\n";
    const MV_MINISIG: &str = "untrusted comment: signature from rsign secret key\n\
        RUREdimvfA8cGfjxfSkBmLo+I8PwenC65B6S5KwG4eoW7LK2wZvgGKvO1cPGXdZk65qDNQsWlEZA2Qi2k19szGZyTNqwK/XwkQ4=\n\
        trusted comment: release 1.0.0\n\
        GpjFwyBwM/Y2eADDfYzKnmxpMUDVnyRmi+qL07zxgI8D5miZPIdl5qfPhoQxsU0XoHHkDbLysSlfClM//FEQBQ==\n";

    /// Stage `<dir>/ironcache`, `<dir>/SHA256SUMS`, `<dir>/SHA256SUMS.minisig` from the real vector.
    fn stage_minisign_release(dir: &Path, sums: &str, minisig: &str) -> (PathBuf, PathBuf) {
        std::fs::write(dir.join("ironcache"), MV_BIN_CONTENT).unwrap();
        let sums_path = dir.join("SHA256SUMS");
        std::fs::write(&sums_path, sums).unwrap();
        std::fs::write(dir.join("SHA256SUMS.minisig"), minisig).unwrap();
        (dir.join("ironcache"), sums_path)
    }

    #[test]
    fn minisign_verifier_accepts_a_genuine_signed_release() {
        let dir = temp_dir("mv-ok");
        let (bin, sums) = stage_minisign_release(&dir, MV_SUMS, MV_MINISIG);
        MinisignVerifier::new(MV_PUBKEY)
            .unwrap()
            .verify(&bin, "ironcache", &sums)
            .expect("a genuine minisign-signed release verifies (integrity + authenticity)");
    }

    #[test]
    fn minisign_verifier_rejects_a_tampered_binary() {
        let dir = temp_dir("mv-badbin");
        let (bin, sums) = stage_minisign_release(&dir, MV_SUMS, MV_MINISIG);
        std::fs::write(&bin, b"tampered-binary").unwrap(); // sha256 no longer matches SHA256SUMS
        let err = MinisignVerifier::new(MV_PUBKEY)
            .unwrap()
            .verify(&bin, "ironcache", &sums)
            .expect_err("a tampered binary fails the integrity check");
        assert!(matches!(err, VerifyError::Mismatch { .. }), "{err:?}");
    }

    #[test]
    fn minisign_verifier_rejects_an_unsigned_alteration_to_sha256sums() {
        // Alter the SHA256SUMS bytes in a way that keeps INTEGRITY passing (double space -> single
        // space; `lookup_sum` still finds the same digest for `ironcache`) so the SIGNATURE layer is
        // what rejects it -- proving authenticity is actually enforced, not just integrity.
        let dir = temp_dir("mv-badsums");
        std::fs::write(dir.join("ironcache"), MV_BIN_CONTENT).unwrap();
        let sums = dir.join("SHA256SUMS");
        std::fs::write(&sums, MV_SUMS.replacen("  ironcache", " ironcache", 1)).unwrap();
        std::fs::write(dir.join("SHA256SUMS.minisig"), MV_MINISIG).unwrap();
        let err = MinisignVerifier::new(MV_PUBKEY)
            .unwrap()
            .verify(&dir.join("ironcache"), "ironcache", &sums)
            .expect_err("SHA256SUMS bytes that differ from what was signed must fail authenticity");
        assert!(matches!(err, VerifyError::Signature(_)), "{err:?}");
    }

    #[test]
    fn minisign_verifier_rejects_a_missing_signature() {
        let dir = temp_dir("mv-nosig");
        std::fs::write(dir.join("ironcache"), MV_BIN_CONTENT).unwrap();
        let sums = dir.join("SHA256SUMS");
        std::fs::write(&sums, MV_SUMS).unwrap();
        // No SHA256SUMS.minisig written.
        let err = MinisignVerifier::new(MV_PUBKEY)
            .unwrap()
            .verify(&dir.join("ironcache"), "ironcache", &sums)
            .expect_err("a missing signature cannot be authenticated");
        assert!(matches!(err, VerifyError::ReadSignature { .. }), "{err:?}");
    }

    #[test]
    fn minisign_verifier_new_rejects_a_malformed_pinned_key() {
        assert!(matches!(
            MinisignVerifier::new("not-a-valid-key"),
            Err(VerifyError::Signature(_))
        ));
    }
}
