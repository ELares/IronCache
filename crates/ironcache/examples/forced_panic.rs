// SPDX-License-Identifier: MIT OR Apache-2.0
//! Forced-panic DEMONSTRATION for the crash-ergonomics panic hook (#551).
//!
//! This tiny example installs the SAME [`ironcache::panic_hook::install_panic_hook`] the server
//! installs at boot, then deliberately panics, so a reviewer can SEE the crash-ergonomics behavior
//! on a real `--release` build (where `panic = "abort"` and the binary is size-stripped):
//!
//! ```sh
//! # Function names in the backtrace need the retained symbol table (strip = "debuginfo").
//! RUST_BACKTRACE=1 cargo run -p ironcache --example forced_panic --release
//! ```
//!
//! Expected on stderr: the hook's ONE actionable `ERROR` line (the panic message, the
//! `file:line` location, the build version, and the report URL), followed -- because
//! `RUST_BACKTRACE=1` is set -- by a backtrace whose frames resolve to FUNCTION NAMES (including
//! `forced_panic::boom`), then the process aborts. This is the live proof of the acceptance for
//! #551; the pure summary shape is also unit-tested in `panic_hook.rs`.

fn main() {
    // A minimal stderr `tracing` subscriber so the hook's `tracing::error!` is visible (the server
    // installs its own via `install_tracing`; here we stand up a bare one for the demo).
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    // Install the real hook, then trigger the panic through a `#[inline(never)]` frame so the named
    // function shows up in the symbolized backtrace.
    ironcache::panic_hook::install_panic_hook("forced-panic-demo");
    boom();
}

#[inline(never)]
fn boom() {
    panic!("forced panic to demonstrate the crash-ergonomics hook (#551)");
}
