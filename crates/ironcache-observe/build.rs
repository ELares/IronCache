// SPDX-License-Identifier: MIT OR Apache-2.0
//! Build script for `ironcache-observe`.
//!
//! Its only job is to tell Cargo to re-run the build when `IRONCACHE_BUILD_VERSION`
//! changes, so the `option_env!` read of it in `SERVER_VERSION` (the version reported
//! in `INFO` / `HELLO` / `LOLWUT`) re-stamps a cached `target/` instead of baking a
//! stale value. Cargo does not track arbitrary env vars for rebuild invalidation, so
//! without this a second build that only changes the stamped version (the
//! rolling-release case, and the #630 two-version rolling-upgrade smoke) could keep the
//! previously compiled version string. Mirrors the `ironcache` binary's build script so
//! `INFO ironcache_version` and the CLI `--version` banner re-stamp together.

fn main() {
    println!("cargo:rerun-if-env-changed=IRONCACHE_BUILD_VERSION");
}
