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

/// The version string reported by `--version`/`-V` and the server boot banner.
///
/// Prefers the compile-time `IRONCACHE_BUILD_VERSION` (the rolling-release
/// workflow stamps the calendar version `YYYY.MMDD.N` there, see RELEASING.md)
/// and otherwise the workspace package version `CARGO_PKG_VERSION` (the normal
/// dev/CI/test case, where the lockfile pins every crate at `0.0.0`).
/// `option_env!` is read at compile time and never touches `Cargo.lock`, so it
/// cannot break `cargo build --locked` the way bumping the `Cargo.toml` version
/// would. `build.rs` emits `rerun-if-env-changed=IRONCACHE_BUILD_VERSION`, so a
/// cached target still re-stamps when the variable changes rather than baking a
/// stale value.
pub const BUILD_VERSION: &str = match option_env!("IRONCACHE_BUILD_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// Top-level CLI. The default subcommand (none given) is `server`, matching
/// `ironcache server` zero-config boot (CLI_BINARY.md).
#[derive(Debug, Parser)]
#[command(
    name = "ironcache",
    version = BUILD_VERSION,
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

    /// Per-shard async I/O backend: `tokio` (default, portable) or `io_uring` (PROD-10 / #28,
    /// Linux-only + requires the `io_uring` build feature; falls back to tokio otherwise).
    #[arg(long, global = true, value_name = "BACKEND")]
    pub runtime: Option<String>,

    /// The subcommand. Absent means `server`.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// The six modes (ADR-0020).
// The `Upgrade` variant carries the (intentionally flat, many-flag) `UpgradeArgs`, so it is far larger
// than the unit variants. This enum is parsed ONCE at process start, not stored in bulk, so the size
// difference is immaterial and boxing it would only obscure the clap `Subcommand` derive.
#[allow(clippy::large_enum_variant)]
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
    /// Self-update: swap the on-disk binary to a new version and restart the systemd-managed
    /// server onto it, DATA-SAFELY (SAVE-first) and SAFELY (sha256-verify + health-gate +
    /// auto-rollback). Operator-run + privileged; NOT a RESP surface. The signature anchor (#386),
    /// HTTPS auto-fetch, and the lossless write-freeze (#388) are explicit follow-ups (#387).
    Upgrade(UpgradeArgs),
}

/// Arguments for `ironcache upgrade` (the #387 mechanism). `--binary` + `--sha256sums` are the v1
/// local source + integrity manifest; the swap target, unit, health, auth, and rollback knobs all
/// have safe defaults.
// The four flag fields (`--no-rollback`, `--yes`, `--allow-same`, `--no-freeze`) are independent
// operator toggles on a clap argument bag, not a state machine; the bool-per-flag shape is the clap
// idiom and the clearest surface here.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, clap::Args)]
pub struct UpgradeArgs {
    /// Path to the NEW ironcache binary to install (the LOCAL source). Its sha256 must match its
    /// entry in `--sha256sums`, and it must run + report a version. Mutually exclusive with the
    /// remote `--from-url` fetch (#394); exactly one source must be given.
    #[arg(long, value_name = "PATH")]
    pub binary: Option<PathBuf>,

    /// Path to the release `SHA256SUMS` to verify `--binary` against (its sha256 must equal the
    /// entry whose file name matches `--binary`'s basename). Required with `--binary`.
    #[arg(long, value_name = "PATH")]
    pub sha256sums: Option<PathBuf>,

    /// REMOTE source (#394): fetch the release TARBALL from this HTTPS URL instead of a local
    /// `--binary`. The tarball is downloaded (bounded), verified against the `--sums-url`
    /// `SHA256SUMS`, and its `ironcache` binary extracted, then installed through the SAME verified /
    /// SAVE-first / health-gated / auto-rollback flow as the local path. Requires `--sums-url`;
    /// mutually exclusive with `--binary`.
    #[arg(long, value_name = "URL")]
    pub from_url: Option<String>,

    /// The HTTPS URL of the release `SHA256SUMS` that vouches for the `--from-url` tarball. Required
    /// when `--from-url` is given.
    #[arg(long, value_name = "URL")]
    pub sums_url: Option<String>,

    /// The live binary path to swap onto (the `.new`/`.old` slots live alongside it on the SAME
    /// filesystem). Defaults to the systemd unit's `ExecStart` path.
    #[arg(long, value_name = "PATH", default_value = "/usr/local/bin/ironcache")]
    pub target: PathBuf,

    /// The systemd unit to restart onto the new binary.
    #[arg(long, value_name = "NAME", default_value = "ironcache")]
    pub unit: String,

    /// The ops endpoint serving `/readyz` to health-probe after the restart.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:9121")]
    pub readyz_addr: String,

    /// The RESP `host:port` for the SAVE-first connection and the `PING` health probe.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:6379")]
    pub resp_addr: String,

    /// Path to a file holding the `requirepass` password for the loopback SAVE / PING connections
    /// (kept out of argv/logs; the password is read from this FILE, never passed on the command
    /// line).
    #[arg(long, value_name = "PATH")]
    pub auth_file: Option<PathBuf>,

    /// How long (seconds) to wait for the restarted server to come back ready + on the expected
    /// version before failing the upgrade (and, by default, auto-rolling-back).
    #[arg(long, value_name = "SECS", default_value_t = 30)]
    pub health_timeout: u64,

    /// Skip auto-rollback on a failed health gate: leave the new binary in place and report failure
    /// (for an operator who wants to debug the new binary in situ).
    #[arg(long)]
    pub no_rollback: bool,

    /// Skip the confirm prompt. ALSO permits proceeding when persistence is not configured
    /// (accepting the in-memory data loss across the restart) and with a same-version target.
    #[arg(long)]
    pub yes: bool,

    /// Permit upgrading to the SAME version already installed (a re-install / repair) without
    /// `--yes`. Off by default, since a same-version upgrade is usually a mistake.
    #[arg(long)]
    pub allow_same: bool,

    /// Opt OUT of the lossless write-freeze (#388). By default `ironcache upgrade` issues a node-wide
    /// `CLIENT PAUSE WRITE` before the final SAVE so NO acknowledged write is lost across the upgrade.
    /// `--no-freeze` skips that and behaves exactly as before (SAVE-first only), accepting the tiny
    /// window where the old process can ack a write that is not in the snapshot -- for a read-mostly
    /// or rebuildable cache where availability matters more than that window.
    #[arg(long)]
    pub no_freeze: bool,
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
    fn build_version_matches_option_env_fallback() {
        // BUILD_VERSION is IRONCACHE_BUILD_VERSION when the rolling-release
        // workflow stamps it, and CARGO_PKG_VERSION otherwise. Mirror that exact
        // fallback so this holds in both worlds (dev/CI where the var is unset,
        // and a stamped release build where it is set).
        let expected = option_env!("IRONCACHE_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        assert_eq!(BUILD_VERSION, expected);
        assert!(!BUILD_VERSION.is_empty());
    }

    #[test]
    fn clap_version_is_wired_to_build_version() {
        // The `version = BUILD_VERSION` attribute must actually reach clap, so
        // `--version` prints the stamped build and not Cargo's `0.0.0`.
        assert_eq!(Cli::command().get_version(), Some(BUILD_VERSION));
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
