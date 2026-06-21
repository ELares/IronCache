// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache Console binary entry point (issue #353).
//!
//! Thin wrapper: parse the CLI and hand off to [`ironcache_console::run_cli`].
//! All wiring lives in the library half so `tests/` can drive the real server.
#![forbid(unsafe_code)]

use clap::Parser as _;

fn main() -> anyhow::Result<()> {
    ironcache_console::run_cli(&ironcache_console::cli::Cli::parse())
}
