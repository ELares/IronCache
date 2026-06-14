// SPDX-License-Identifier: MIT OR Apache-2.0
//! Clap CLI surface (CLI_BINARY.md, ADR-0020).
//!
//! One binary, six modes: `server | cli | bench | check | config | upgrade`, with
//! global root flags (`--config`, `--bind`, `--port`, `--log-level`,
//! `--metrics-addr`). The `redis-cli` argv[0] alias forwards to `cli`
//! (ADR-0020 "convenience, not the primary dispatch").

use clap::{Parser, Subcommand};
use std::net::IpAddr;
use std::path::PathBuf;

/// Top-level CLI. The default subcommand (none given) is `server`, matching
/// `ironcache server` zero-config boot (CLI_BINARY.md).
#[derive(Debug, Parser)]
#[command(
    name = "ironcache",
    version,
    about = "The most efficient Redis-compatible cache, in one static binary.",
    propagate_version = true
)]
pub struct Cli {
    /// Path to a TOML config file (optional; an absent file uses defaults).
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Bind address override (root flag, highest non-runtime precedence).
    #[arg(long, global = true, value_name = "IP")]
    pub bind: Option<IpAddr>,

    /// Port override.
    #[arg(long, global = true, value_name = "PORT")]
    pub port: Option<u16>,

    /// Log level (reserved; structured logging lands with observability).
    #[arg(long, global = true, value_name = "LEVEL", default_value = "info")]
    pub log_level: String,

    /// Metrics endpoint address (reserved; /metrics lands with observability).
    #[arg(long, global = true, value_name = "ADDR")]
    pub metrics_addr: Option<String>,

    /// Shard count override (defaults to available parallelism).
    #[arg(long, global = true, value_name = "N")]
    pub shards: Option<usize>,

    /// The subcommand. Absent means `server`.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// The six modes (ADR-0020).
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the cache server (the daemon). This is the default mode.
    Server,
    /// Interactive client REPL (aliased by a `redis-cli` invocation).
    ///
    /// The auto-generated short help is disabled so `-h` carries the redis-cli
    /// meaning (host); use `--help` for help.
    #[command(disable_help_flag = true)]
    Cli {
        /// Host to connect to (redis-cli compatible `-h`).
        #[arg(short = 'h', long, default_value = "127.0.0.1")]
        host: String,
        /// Port to connect to.
        #[arg(short = 'p', long, default_value_t = ironcache_config::DEFAULT_PORT)]
        port: u16,
        /// Print help.
        #[arg(long, action = clap::ArgAction::Help)]
        help: Option<bool>,
    },
    /// Benchmark harness (stub in PR-1).
    Bench,
    /// Validate config and run a self-check.
    Check,
    /// Print the effective configuration.
    Config,
    /// Self-update with verified rollback (stub in PR-1).
    Upgrade,
}

/// Returns true when the binary was invoked under a `redis-cli` basename, in
/// which case dispatch forwards to `cli` (ADR-0020 alias).
#[must_use]
pub fn invoked_as_redis_cli(argv0: &str) -> bool {
    let base = std::path::Path::new(argv0)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    // Strip a trailing .exe for the Windows case.
    let base = base.strip_suffix(".exe").unwrap_or(&base);
    base == "redis-cli"
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        // Catches arg-conflict / id-collision bugs at test time.
        Cli::command().debug_assert();
    }

    #[test]
    fn redis_cli_alias_detection() {
        assert!(invoked_as_redis_cli("redis-cli"));
        assert!(invoked_as_redis_cli("/usr/local/bin/redis-cli"));
        assert!(invoked_as_redis_cli("redis-cli.exe"));
        assert!(!invoked_as_redis_cli("ironcache"));
        assert!(!invoked_as_redis_cli("/opt/ironcache"));
    }

    #[test]
    fn parses_server_with_flags() {
        let cli = Cli::try_parse_from(["ironcache", "--port", "7000", "server"]).unwrap();
        assert_eq!(cli.port, Some(7000));
        assert!(matches!(cli.command, Some(Command::Server)));
    }

    #[test]
    fn no_subcommand_is_allowed() {
        let cli = Cli::try_parse_from(["ironcache"]).unwrap();
        assert!(cli.command.is_none());
    }
}
