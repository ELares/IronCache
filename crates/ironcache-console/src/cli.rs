// SPDX-License-Identifier: MIT OR Apache-2.0
//! Clap CLI surface for the console binary (issue #353).
//!
//! One binary, three modes: `run` (the default, serve the console), `check`
//! (validate config and exit), and `config` (print the effective config and
//! exit). Global flags (`--config`, `--http-addr`, `--log-level`) overlay the
//! TOML file and `IRONCACHE_CONSOLE_*` env, with the CLI highest.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// The version string reported by `--version`/`-V` and the boot banner.
///
/// Prefers the compile-time `IRONCACHE_BUILD_VERSION` (the rolling-release
/// workflow stamps the calendar version `YYYY.MMDD.N` there, shared with the
/// engine, see RELEASING.md) and otherwise the workspace package version
/// `CARGO_PKG_VERSION` (the dev/CI/test case, pinned at `0.0.0`). `option_env!`
/// is read at compile time and never touches `Cargo.lock`, so it cannot break
/// `cargo build --locked`. `build.rs` emits the rerun-if-changed hint so a
/// cached target re-stamps when the variable changes.
pub const BUILD_VERSION: &str = match option_env!("IRONCACHE_BUILD_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// Top-level CLI. The default subcommand (none given) is `run`.
#[derive(Debug, Parser)]
#[command(
    name = "ironcache-console",
    version = BUILD_VERSION,
    about = "IronCache Console: a separate cluster monitoring server for an IronCache deployment.",
    propagate_version = true
)]
pub struct Cli {
    /// Path to a TOML config file (optional; an absent file uses defaults).
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// HTTP listen address override (`host:port`), highest-precedence.
    #[arg(long, global = true, value_name = "ADDR")]
    pub http_addr: Option<String>,

    /// Log level: error | warn | info | debug | trace. Absent here lets the TOML
    /// file / `IRONCACHE_CONSOLE_LOG_LEVEL` show through (CLI is highest); the
    /// effective default when nothing sets it is `info`.
    #[arg(long, global = true, value_name = "LEVEL")]
    pub log_level: Option<String>,

    /// The subcommand. Absent means `run`.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// The three console modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Serve the console (the default mode).
    Run,
    /// Validate the effective config and exit.
    Check,
    /// Print the effective config and exit.
    Config,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    #[test]
    fn cli_definition_is_valid() {
        // Catches arg-conflict / id-collision bugs at test time.
        Cli::command().debug_assert();
    }

    #[test]
    fn build_version_matches_option_env_fallback() {
        let expected = option_env!("IRONCACHE_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        assert_eq!(BUILD_VERSION, expected);
        assert!(!BUILD_VERSION.is_empty());
    }

    #[test]
    fn clap_version_is_wired_to_build_version() {
        assert_eq!(Cli::command().get_version(), Some(BUILD_VERSION));
    }

    #[test]
    fn no_subcommand_is_allowed() {
        let cli = Cli::try_parse_from(["ironcache-console"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn parses_global_flags_before_subcommand() {
        let cli = Cli::try_parse_from([
            "ironcache-console",
            "--http-addr",
            "127.0.0.1:9999",
            "--log-level",
            "debug",
            "check",
        ])
        .unwrap();
        assert_eq!(cli.http_addr.as_deref(), Some("127.0.0.1:9999"));
        assert_eq!(cli.log_level.as_deref(), Some("debug"));
        assert_eq!(cli.command, Some(Command::Check));
    }

    #[test]
    fn log_level_absent_lets_lower_layers_show_through() {
        // No --log-level: the field is None so the TOML/env layer (and finally
        // the `info` default) decides, rather than clap forcing a value.
        let cli = Cli::try_parse_from(["ironcache-console", "run"]).unwrap();
        assert_eq!(cli.log_level, None);
    }
}
