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

    /// Metrics / health endpoint bind (serves `/metrics`, `/livez`, `/readyz`). Absent means the
    /// DEFAULT localhost bind [`DEFAULT_METRICS_ADDR`] (`127.0.0.1:9091`), so a Prometheus scrape and
    /// the k8s probes work out of the box WITHOUT exposing the ops port publicly (#555, the
    /// tunability principle: env-dependent tradeoffs default SAFE, not off). Override with any
    /// `host:port` (e.g. `0.0.0.0:9121` to expose it behind a network policy), or DISABLE the
    /// endpoint with `off` (also `none` / `disabled` / an empty value). Resolution lives in
    /// [`effective_metrics_addr`].
    #[arg(long, global = true, value_name = "ADDR")]
    pub metrics_addr: Option<String>,

    /// Shard count override (defaults to available parallelism).
    #[arg(long, global = true, value_name = "N")]
    pub shards: Option<usize>,

    /// Per-shard async I/O backend: `tokio` (default, portable) or `io_uring` (PROD-10 / #28,
    /// Linux-only + requires the `io_uring` build feature; falls back to tokio otherwise).
    #[arg(long, global = true, value_name = "BACKEND")]
    pub runtime: Option<String>,

    /// Dedicated persist core (#589): which CPU core(s) the off-datapath `ic-persist` thread pins to
    /// during a save, so its encode stops stealing a serving core. `off` (default, no pin), `auto`
    /// (reserve the highest core of the process cpuset), or an explicit cpu list (`8`, `6-7`,
    /// `6-7,10`). Linux-only (a no-op elsewhere). Env `IRONCACHE_PERSIST_CPU`; TOML `persist_cpu`.
    #[arg(long, global = true, value_name = "off|auto|LIST")]
    pub persist_cpu: Option<String>,

    /// #527 config-rollback ESCAPE HATCH: boot past an UNKNOWN config-file key with a loud warning
    /// (one line per key, naming it) instead of hard-failing. OFF by default (strict, so a typo is
    /// caught in normal ops); turn it ON only for a DOWNGRADE, where an old binary must start past a
    /// forward-incompatible key a newer build wrote into the config file rather than bricking the
    /// rollback. Env `IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS`; TOML `ignore_unknown_config_keys`. It
    /// relaxes unknown KEYS only -- a malformed file or a bad VALUE still fails boot.
    #[arg(long, global = true)]
    pub ignore_unknown_config_keys: bool,

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
    /// server onto it, DATA-SAFELY (SAVE-first + lossless write-freeze) and SAFELY (sha256 INTEGRITY,
    /// minisign AUTHENTICITY once a public key is pinned #386, health-gate, auto-rollback).
    /// Operator-run + privileged; NOT a RESP surface.
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

    /// REMOTE source (#394): fetch a specific release TAG or `latest` from GitHub Releases. The value
    /// is the git TAG: a rolling build is tagged by its calendar version (e.g. `2026.0701.1`), while a
    /// formal release is tagged `vX.Y.Z` (INCLUDE the leading `v` -- `--to v1.2.3`, NOT `--to 1.2.3`,
    /// even though `ironcache --version` prints the bare `1.2.3`). Resolves this platform's asset URL
    /// (+ the `SHA256SUMS`), then downloads / verifies / extracts / installs it exactly like
    /// `--from-url`. `latest` follows the `releases/latest` redirect to the newest rolling build.
    /// Mutually exclusive with `--binary` and `--from-url`.
    #[arg(long, value_name = "TAG|latest")]
    pub to: Option<String>,

    /// The `owner/repo` to fetch `--to` from (default `ELares/IronCache`).
    #[arg(long, value_name = "OWNER/REPO")]
    pub repo: Option<String>,

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

    /// CLUSTER MODE (#392): run the live rolling upgrade of a raft-governance CLUSTER (upgrade the
    /// replicas first, promote an upgraded in-sync replica, upgrade the old primary LAST) instead of
    /// the single-node self-upgrade. Requires `--inventory` (the static actuation map) and `--to`
    /// (the explicit target version). The per-node local flags (`--target`, `--unit`, `--resp-addr`,
    /// `--readyz-addr`, `--auth-file`) do not apply on the orchestrator; each node's reach + actuation
    /// comes from the inventory. Off by default (the single-node path is unchanged).
    #[arg(long)]
    pub cluster: bool,

    /// The TOML actuation-map inventory for `--cluster`: each `[[node]]` supplies `id`, `resp_addr`,
    /// optional `auth`, `ssh_target`, `upgrade_source`, plus the observe `seeds`. REQUIRED with
    /// `--cluster`. The dynamic topology (roles / versions / lag / membership) is discovered live.
    #[arg(long, value_name = "FILE")]
    pub inventory: Option<PathBuf>,

    /// CLUSTER MODE in-sync bound (#392): a replica is a promotion candidate when its master-side lag
    /// is `<= --max-lag`. Defaults to the server's own `replica_max_lag`, which the server re-checks
    /// on `CLUSTER FAILOVER`; the failover-freeze drains the candidate to lag 0 regardless, so this is
    /// a pre-filter, not the safety boundary.
    #[arg(long, value_name = "N", default_value_t = ironcache_config::DEFAULT_REPLICA_MAX_LAG)]
    pub max_lag: u64,

    /// CLUSTER MODE failover-freeze drain bound (#392): how long (seconds) to poll the chosen
    /// candidate's master-side lag down to EXACTLY 0 before FAILING CLOSED (unpause, no promotion).
    /// Polled every 100ms, so the default 60s matches the driver's own 600-poll budget.
    #[arg(long, value_name = "SECS", default_value_t = 60)]
    pub drain_timeout: u64,

    /// CLUSTER MODE freeze window (#392): the `CLIENT PAUSE <MS> WRITE` window applied to the old
    /// primary during a promotion (it self-cancels once the old primary is demoted). Must comfortably
    /// cover the drain + the commit + a margin. Matches the driver's default freeze window.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub pause_ms: u64,

    /// CLUSTER MODE per-node RESP timeout (#392): the bound on each authenticated RESP exchange
    /// (`INFO` / `CLUSTER` / `CLUSTER FAILOVER`) to a node, in seconds. A slow / unreachable node
    /// fails the exchange rather than hanging the roll.
    #[arg(long, value_name = "SECS", default_value_t = 30)]
    pub per_node_timeout: u64,

    /// CLUSTER MODE tick budget (#392): the maximum number of rolling-upgrade ticks before the driver
    /// fails LOUD (`StalledAfterBudget`) instead of looping forever -- a replica that never catches up
    /// or a promotion that stays blocked (no quorum / no in-sync candidate) stops here, fail-closed.
    #[arg(long, value_name = "N", default_value_t = 300)]
    pub max_ticks: usize,

    /// CLUSTER MODE preview (#392): OBSERVE the cluster once and print the derived plan (the current
    /// versions, the replica roll order, the promotion candidate, the primary upgraded LAST) then EXIT
    /// -- taking NO action (no upgrade, no failover). Use it to confirm primary-last before committing.
    #[arg(long)]
    pub dry_run: bool,
}

impl UpgradeArgs {
    /// The `--cluster` inputs that are REQUIRED in cluster mode: the inventory path and the explicit
    /// `--to` target (dev / lock builds pin a constant version, so an explicit target is mandatory).
    /// Returns a clear error message when `--cluster` is set without one of them. The single-node
    /// path never calls this, so its behavior is unchanged.
    ///
    /// # Errors
    /// A message naming the missing required flag.
    pub fn require_cluster_inputs(&self) -> Result<(&std::path::Path, &str), String> {
        let inventory = self.inventory.as_deref().ok_or_else(|| {
            "`--cluster` requires `--inventory <FILE>` (the TOML actuation map)".to_owned()
        })?;
        let target = self.to.as_deref().ok_or_else(|| {
            "`--cluster` requires `--to <TAG>` (the explicit target version; dev/lock builds pin a \
             constant version, so it cannot be inferred)"
                .to_owned()
        })?;
        Ok((inventory, target))
    }
}

/// The DEFAULT metrics / health endpoint bind (#555). A LOCALHOST address so `/metrics`, `/livez`,
/// and `/readyz` are scrapable out of the box WITHOUT exposing the ops port publicly, per the
/// tunability principle (env-dependent tradeoffs default SAFE, not off). Override with any
/// `host:port` (e.g. `0.0.0.0:9121` to expose it behind a network policy) or disable it with `off`;
/// [`effective_metrics_addr`] applies the policy. Chosen distinct from the RESP port (6379) and from
/// the deployment artifacts' publicly-exposed `9121` so a local default cannot collide with them.
pub const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9091";

/// Resolve the EFFECTIVE metrics bind from the (optional) `--metrics-addr` value:
///
///   * absent (`None`) -> the default localhost bind [`DEFAULT_METRICS_ADDR`] (endpoint ON),
///   * a disable sentinel (`off` / `none` / `disabled` / empty, case-insensitive) -> `None` (OFF),
///   * any other value -> that `host:port` (the operator's override).
///
/// This is the single place the default-on-localhost policy is applied, keeping it OVERRIDABLE and
/// DISABLE-ABLE (the tunability principle: never a baked-in one-way choice). `None` in the return
/// means the endpoint is disabled and no listener is bound.
#[must_use]
pub fn effective_metrics_addr(raw: Option<&str>) -> Option<&str> {
    let value = raw.unwrap_or(DEFAULT_METRICS_ADDR).trim();
    let disabled = value.is_empty()
        || value.eq_ignore_ascii_case("off")
        || value.eq_ignore_ascii_case("none")
        || value.eq_ignore_ascii_case("disable")
        || value.eq_ignore_ascii_case("disabled");
    if disabled { None } else { Some(value) }
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
    fn parses_ignore_unknown_config_keys_flag() {
        // #527 config-rollback escape hatch: OFF by default, ON when the flag is present (a global
        // flag, so it works before OR after the subcommand).
        let off = Cli::try_parse_from(["ironcache", "server"]).unwrap();
        assert!(!off.ignore_unknown_config_keys, "strict by default");
        let on =
            Cli::try_parse_from(["ironcache", "--ignore-unknown-config-keys", "server"]).unwrap();
        assert!(on.ignore_unknown_config_keys, "flag flips the hatch on");
    }

    #[test]
    fn no_subcommand_is_allowed() {
        let cli = Cli::try_parse_from(["ironcache"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn metrics_addr_defaults_on_localhost() {
        // #555, tunability principle: an ABSENT `--metrics-addr` resolves to the localhost default
        // (endpoint ON, scrapable out of the box, not publicly exposed) -- the safe default, not off.
        assert_eq!(effective_metrics_addr(None), Some(DEFAULT_METRICS_ADDR));
        assert_eq!(effective_metrics_addr(None), Some("127.0.0.1:9091"));
    }

    #[test]
    fn metrics_addr_override_is_honored() {
        // An explicit bind overrides the default (e.g. exposing it behind a network policy).
        assert_eq!(
            effective_metrics_addr(Some("0.0.0.0:9121")),
            Some("0.0.0.0:9121")
        );
        // Surrounding whitespace is trimmed so a config-templated value is not mis-parsed.
        assert_eq!(
            effective_metrics_addr(Some("  127.0.0.1:9091  ")),
            Some("127.0.0.1:9091")
        );
    }

    /// Parse an `ironcache upgrade ...` argv into its [`UpgradeArgs`] (via the real clap surface).
    fn parse_upgrade(argv: &[&str]) -> UpgradeArgs {
        let full: Vec<&str> = ["ironcache", "upgrade"]
            .into_iter()
            .chain(argv.iter().copied())
            .collect();
        match Cli::try_parse_from(full)
            .expect("upgrade args parse")
            .command
        {
            Some(Command::Upgrade(a)) => a,
            other => panic!("expected the upgrade subcommand, got {other:?}"),
        }
    }

    #[test]
    fn cluster_flags_parse_into_the_expected_values() {
        let args = parse_upgrade(&[
            "--cluster",
            "--inventory",
            "/etc/ironcache/cluster.toml",
            "--to",
            "v1.2.3",
            "--max-lag",
            "8",
            "--drain-timeout",
            "15",
            "--pause-ms",
            "45000",
            "--per-node-timeout",
            "20",
            "--max-ticks",
            "500",
            "--dry-run",
        ]);
        assert!(args.cluster);
        assert!(args.dry_run);
        assert_eq!(
            args.inventory.as_deref(),
            Some(std::path::Path::new("/etc/ironcache/cluster.toml"))
        );
        assert_eq!(args.to.as_deref(), Some("v1.2.3"));
        assert_eq!(args.max_lag, 8);
        assert_eq!(args.drain_timeout, 15);
        assert_eq!(args.pause_ms, 45_000);
        assert_eq!(args.per_node_timeout, 20);
        assert_eq!(args.max_ticks, 500);
    }

    #[test]
    fn cluster_tuning_flags_have_sensible_defaults() {
        // The driver-tuning knobs default to match the driver / server defaults so an operator can
        // run `--cluster --inventory ... --to ...` with no tuning.
        let args = parse_upgrade(&["--cluster", "--inventory", "/tmp/inv.toml", "--to", "v1"]);
        assert_eq!(args.max_lag, ironcache_config::DEFAULT_REPLICA_MAX_LAG);
        assert_eq!(args.drain_timeout, 60);
        assert_eq!(args.pause_ms, 30_000);
        assert_eq!(args.per_node_timeout, 30);
        assert_eq!(args.max_ticks, 300);
        assert!(!args.dry_run);
    }

    #[test]
    fn cluster_without_inventory_is_a_clear_error() {
        let args = parse_upgrade(&["--cluster", "--to", "v1.2.3"]);
        let err = args
            .require_cluster_inputs()
            .expect_err("cluster needs --inventory");
        assert!(err.contains("--inventory"), "{err}");
    }

    #[test]
    fn cluster_without_to_is_a_clear_error() {
        let args = parse_upgrade(&["--cluster", "--inventory", "/tmp/inv.toml"]);
        let err = args
            .require_cluster_inputs()
            .expect_err("cluster needs --to");
        assert!(err.contains("--to"), "{err}");
    }

    #[test]
    fn cluster_inputs_resolve_when_both_present() {
        let args = parse_upgrade(&[
            "--cluster",
            "--inventory",
            "/tmp/inv.toml",
            "--to",
            "v1.2.3",
        ]);
        let (inv, target) = args.require_cluster_inputs().expect("both present");
        assert_eq!(inv, std::path::Path::new("/tmp/inv.toml"));
        assert_eq!(target, "v1.2.3");
    }

    #[test]
    fn single_node_upgrade_still_parses_unchanged() {
        // The single-node path is untouched: no --cluster, the source flags still parse, and cluster
        // mode is off with the tuning knobs at their defaults.
        let args = parse_upgrade(&["--binary", "/tmp/ic", "--sha256sums", "/tmp/SUMS"]);
        assert!(!args.cluster, "single-node is not cluster mode");
        assert!(args.inventory.is_none());
        assert_eq!(
            args.binary.as_deref(),
            Some(std::path::Path::new("/tmp/ic"))
        );
    }

    #[test]
    fn metrics_addr_disable_sentinels_turn_it_off() {
        // The endpoint stays DISABLE-ABLE (tunability): a sentinel value binds no listener.
        for raw in [
            "off", "OFF", "Off", "none", "None", "disable", "disabled", "", "   ",
        ] {
            assert_eq!(
                effective_metrics_addr(Some(raw)),
                None,
                "'{raw}' should disable the metrics endpoint"
            );
        }
    }
}
