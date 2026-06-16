// SPDX-License-Identifier: MIT OR Apache-2.0
//! Build script for the `ironcache` binary.
//!
//! Its only job is to tell Cargo to re-run the build when
//! `IRONCACHE_BUILD_VERSION` changes, so the `option_env!` read of it in
//! `cli::BUILD_VERSION` re-stamps a cached `target/` instead of baking a stale
//! value. Cargo does not track arbitrary env vars for rebuild invalidation, so
//! without this a second build that only changes the stamped version (the
//! rolling-release case) could keep the previously compiled version string. The
//! rolling-release workflow sets `IRONCACHE_BUILD_VERSION` to the calendar
//! version `YYYY.MMDD.N`; it is unset for dev/CI builds, which fall back to
//! `CARGO_PKG_VERSION` (the lockfile-pinned `0.0.0`).

fn main() {
    println!("cargo:rerun-if-env-changed=IRONCACHE_BUILD_VERSION");
}
