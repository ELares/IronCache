// SPDX-License-Identifier: MIT OR Apache-2.0
//! Re-stamp the build when the release version variable changes. The console
//! shares the engine's `IRONCACHE_BUILD_VERSION` stamp (see RELEASING.md), read
//! at compile time by `cli::BUILD_VERSION` via `option_env!`, so a cached target
//! still picks up a new calendar version rather than baking a stale one. Reading
//! it here never touches `Cargo.lock`, so `cargo build --locked` is unaffected.

fn main() {
    println!("cargo:rerun-if-env-changed=IRONCACHE_BUILD_VERSION");
}
