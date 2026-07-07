// SPDX-License-Identifier: MIT OR Apache-2.0
//! Boot-time PANIC HOOK for crash ergonomics (troubleshooting, #551).
//!
//! The release profile is `panic = "abort"` (CLI_BINARY.md, ADR-0017): a panic in a release build
//! terminates the process with `abort()` and NO orderly stack unwind. Without a hook the last the
//! operator sees is the raw `abort`, so a 3am crash gives no clue WHERE it happened. This module
//! installs, once at boot (before any listener binds), a process-wide hook that writes ONE
//! actionable `tracing::error!` line the instant a panic fires and BEFORE the abort:
//!
//!   * the panic MESSAGE (the payload string), so the operator sees the assertion that tripped;
//!   * the panic LOCATION (`file:line:col`, from [`std::panic::Location`]) -- this is baked into the
//!     binary as static string data and is therefore symbolized REGARDLESS of `strip`, so it is
//!     always present even on the size-stripped release artifact;
//!   * the BUILD VERSION (`cli::BUILD_VERSION`), so a bug report names the exact build; and
//!   * a "report at <issues URL>" line, so the last words are directly actionable.
//!
//! When `RUST_BACKTRACE` is set the hook additionally logs a captured [`Backtrace`]. Its frames
//! resolve to FUNCTION NAMES on a build that retains the symbol table -- the release profile keeps
//! it (`strip = "debuginfo"`, not `"symbols"`), so a from-source / glibc `cargo build --release`
//! gives named frames; that profile choice is the other half of #551. NOTE: the published
//! static-musl artifact is stripped FULLY by its `zig cc` release linker, so on that binary the
//! backtrace frames are raw addresses and the hook's own `file:line` LOCATION (above) is the crash
//! site. See DEPLOY.md "Crash troubleshooting" for the operator runbook (set `RUST_BACKTRACE=1`).
//!
//! Determinism: this is BOOT / PANIC-PATH code in the binary crate, outside the ADR-0003 engine
//! determinism boundary. It reads no clock and no RNG, so it needs no `ironcache-env` seam; the
//! panic location + version are compile-time constants and the backtrace is captured from the OS
//! stack, none of which is a decision the deterministic engine observes.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::panic::PanicHookInfo;

/// The IronCache issue tracker an operator reports a crash at (surfaced in the panic hook's final
/// log line so the "last words" of a crashing process are directly actionable).
pub const REPORT_URL: &str = "https://github.com/ELares/IronCache/issues";

/// Install the process-wide panic hook (#551). Call ONCE at boot, after the `tracing` subscriber is
/// installed and before any listener binds, so a panic anywhere in the process emits the actionable
/// crash line through the same log sink as the rest of the operational logs.
///
/// `version` is the build stamp (`cli::BUILD_VERSION`), threaded in because it lives in the binary's
/// `cli` module rather than this library half. The hook keeps the DEFAULT post-hook behavior
/// (`panic = "abort"` in release aborts once the hook returns); it only ADDS the log line, it does
/// not swallow the panic.
pub fn install_panic_hook(version: &'static str) {
    std::panic::set_hook(Box::new(move |info| {
        // `Backtrace::capture()` honors `RUST_BACKTRACE`/`RUST_LIB_BACKTRACE`: it captures a trace
        // only when the operator opted in, so the default (unset) path stays cheap and quiet.
        let backtrace = Backtrace::capture();
        report_panic(info, version, &backtrace);
    }));
}

/// Emit the crash line(s) for a fired panic. Split out from the closure so the field extraction is
/// straightforward; the human-readable summary is built by the pure [`panic_summary`] (unit-tested).
fn report_panic(info: &PanicHookInfo<'_>, version: &str, backtrace: &Backtrace) {
    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()));
    let message = payload_message(info);

    // ONE actionable line: version + location + message + the report URL. Logged at ERROR through
    // the same `tracing` sink as every other operational log, so it lands wherever stderr goes
    // (journald under the systemd unit) even though the process is about to abort.
    tracing::error!(
        target: "ironcache::panic",
        version = version,
        location = location.as_deref().unwrap_or("unknown"),
        "{}",
        panic_summary(&message, location.as_deref(), version)
    );

    // The backtrace is present only when the operator set `RUST_BACKTRACE` (see DEPLOY.md). Its
    // frames resolve to function names via the retained symbol table (`strip = "debuginfo"`).
    if backtrace.status() == BacktraceStatus::Captured {
        tracing::error!(target: "ironcache::panic", "panic backtrace:\n{backtrace}");
    }
}

/// Extract the panic MESSAGE from the payload. `panic!`/`assert!` carry either a `&'static str` (a
/// literal message) or a `String` (a formatted one); anything else degrades to a placeholder rather
/// than losing the crash line.
fn payload_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_owned()
    }
}

/// Build the single-line, actionable crash summary. PURE (no I/O, no clock/RNG), so the exact shape
/// -- version, location, message, and the report URL all present -- is unit-tested without firing a
/// real panic or installing a global hook.
fn panic_summary(message: &str, location: Option<&str>, version: &str) -> String {
    format!(
        "ironcache PANICKED and is aborting: {message} \
         (at {loc}; build {version}). Please report this crash at {REPORT_URL}",
        loc = location.unwrap_or("an unknown location"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_contains_the_actionable_fields() {
        let s = panic_summary(
            "assertion failed: x > 0",
            Some("src/serve.rs:42:9"),
            "2026.0707.1",
        );
        assert!(
            s.contains("assertion failed: x > 0"),
            "message present: {s}"
        );
        assert!(s.contains("src/serve.rs:42:9"), "location present: {s}");
        assert!(s.contains("2026.0707.1"), "version present: {s}");
        assert!(s.contains(REPORT_URL), "report URL present: {s}");
        assert!(s.contains("PANICKED"), "flags the crash loudly: {s}");
    }

    #[test]
    fn summary_tolerates_a_missing_location() {
        // `PanicHookInfo::location()` is `Option`; a `None` must still produce a usable line.
        let s = panic_summary("boom", None, "0.0.0");
        assert!(s.contains("boom"), "{s}");
        assert!(s.contains("an unknown location"), "{s}");
        assert!(s.contains(REPORT_URL), "{s}");
    }

    #[test]
    fn install_panic_hook_does_not_panic() {
        // Installing the hook must be safe to call at boot; it replaces the process hook. We do not
        // trigger a panic here (release is `panic = abort`; a fired panic would abort the test
        // process). The live end-to-end proof is `examples/forced_panic.rs`, built `--release`.
        install_panic_hook("test-version");
        // Restore the default hook so a later panic in an unrelated test prints normally.
        let _ = std::panic::take_hook();
    }
}
