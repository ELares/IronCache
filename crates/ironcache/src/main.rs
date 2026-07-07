// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache binary entry point (CLI_BINARY.md, ADR-0020).
//!
//! One static binary, six modes. The `server` mode (the default) boots the
//! shared-nothing thread-per-core runtime and serves Tier-0 RESP. The default
//! global allocator is jemalloc (ADR-0006). Signal handling for graceful shutdown
//! lives here in the binary, never in the library crates, so the determinism
//! boundary (ADR-0003) holds.

mod cli;
mod fd_budget;

use anyhow::Context as _;
use clap::Parser;
use cli::{Cli, Command};
// The server wiring lives in the crate's library half (`src/lib.rs`) so integration
// tests can boot the real `run_server`; the binary consumes the same modules here.
use ironcache::metrics_http::{self, MetricsState, ReadyState};
use ironcache::serve;
use ironcache_config::{Config, ConfigOverlay};
use ironcache_observe::MetricsRegistry;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

// jemalloc as the global allocator (ADR-0006). Not available on MSVC; the static
// release targets (musl, macos) all have it.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Build-time jemalloc tuning (ADR-0006). Exporting the `malloc_conf` C string is
// the tikv-jemallocator-sanctioned way to set boot options without env vars. We
// enable the background purge thread by default (flipping jemalloc's upstream-off
// default, the way Redis does) and lower `dirty_decay_ms` below the stock 10 s so
// dirty pages return to the OS faster under eviction churn. The exact decay value
// is a config knob (#85); 5 s is a sensible sub-10 s default until that lands.
//
// tikv-jemalloc-sys builds jemalloc with the `_rjem_` prefix on our targets
// (musl/macos; macOS forces prefixing), so the symbol downstream is
// `_rjem_malloc_conf`. Same cfg-gate as the allocator so MSVC (which has no
// jemalloc here) is unaffected.
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    // background_thread:true enables the async purge thread; dirty_decay_ms:5000
    // returns dirty pages after 5 s (sub-10 s per ADR-0006). `c"..."` is a
    // NUL-terminated C string literal; jemalloc reads this pointer at init.
    &*c"background_thread:true,dirty_decay_ms:5000"
        .as_ptr()
        .cast::<libc::c_char>()
});

fn main() -> anyhow::Result<()> {
    // redis-cli argv[0] alias: forward to `cli` (ADR-0020). If invoked as
    // `redis-cli`, we synthesize a `cli` invocation from the remaining args.
    let argv0 = std::env::args().next().unwrap_or_default();
    if cli::invoked_as_redis_cli(&argv0) {
        return run_cli_alias();
    }

    let cli = Cli::parse();

    // STRUCTURED LOGGING (OBSERVABILITY.md, #152): install ONE tracing subscriber at boot,
    // filtered by `--log-level`, BEFORE any operational log fires. The operational `tracing::*`
    // logs (boot banner, errors, the cluster-tls/dns/persistence/shutdown messages) are then
    // FILTERED by this level (the previously-dead `--log-level` flag becomes live). The sink is
    // stderr (orchestrator-friendly). Installed for EVERY mode so a `cli`/`check`/`config`
    // invocation also honors the level; the default level preserves the prior effective
    // verbosity (info). A failed install (a second subscriber in the same process, e.g. a test
    // harness) is non-fatal: the existing subscriber stands.
    install_tracing(&cli.log_level);

    // CRASH ERGONOMICS (#551): install the process-wide panic hook now, right after the tracing
    // sink is up and BEFORE any listener binds. The release profile is `panic = "abort"`, so a
    // panic terminates the process with no orderly unwind; this hook makes the LAST log line
    // actionable (panic message + `file:line` location + build version + a report URL) instead of a
    // bare abort. Boot/panic-path, outside the ADR-0003 determinism boundary (no clock/RNG).
    ironcache::panic_hook::install_panic_hook(cli::BUILD_VERSION);

    let command = cli.command.as_ref();

    match command {
        None | Some(Command::Server) => cmd_server(&cli),
        Some(Command::Cli { host, port, .. }) => cmd_cli(host, *port),
        Some(Command::Bench) => {
            cmd_bench();
            Ok(())
        }
        Some(Command::Check) => cmd_check(&cli),
        Some(Command::Config) => cmd_config(&cli),
        Some(Command::Upgrade(args)) => cmd_upgrade(args),
    }
}

/// Install the process-wide `tracing` subscriber filtered by `--log-level` (OBSERVABILITY.md,
/// #152), writing to STDERR. Maps the level string (`error`/`warn`/`info`/`debug`/`trace`, the
/// Redis `loglevel` vocabulary plus the standard extras) to a `LevelFilter`; an unrecognized
/// value falls back to `info` (the prior default) with a one-line note, rather than failing boot.
///
/// A suppressed `debug!`/`trace!` short-circuits at the level check BEFORE its arguments are
/// formatted, so the level gate adds no allocation on a quiet hot path. Installing is best-effort:
/// a second install in the same process (a test that already set a global subscriber) is ignored,
/// so this never panics a re-entrant harness.
fn install_tracing(log_level: &str) {
    use tracing_subscriber::fmt;

    let (level, unknown) = parse_log_level(log_level);
    let subscriber = fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        // Compact, timestamp-free lines keep the orchestrator log clean (the platform adds its
        // own timestamps); the target is the module path, which is plenty for ops triage.
        .with_target(true)
        .finish();
    // `try_init`-style: ignore an "already set" error so a re-entrant test harness is safe.
    if tracing::subscriber::set_global_default(subscriber).is_ok() && unknown {
        tracing::warn!(
            requested = log_level,
            "unknown --log-level; defaulting to info"
        );
    }
}

/// Map a `--log-level` string to a `LevelFilter` (OBSERVABILITY.md, #152). Returns the filter and
/// a `bool` that is `true` when the input was UNRECOGNIZED (the caller falls back to `info` and
/// logs a note). The vocabulary is the Redis `loglevel` names plus the standard tracing extras;
/// matching is case-insensitive. Pure, so the level mapping is unit-tested without installing a
/// global subscriber.
fn parse_log_level(log_level: &str) -> (tracing::level_filters::LevelFilter, bool) {
    use tracing::level_filters::LevelFilter;
    match log_level.to_ascii_lowercase().as_str() {
        "error" => (LevelFilter::ERROR, false),
        "warn" | "warning" => (LevelFilter::WARN, false),
        "info" => (LevelFilter::INFO, false),
        "debug" | "verbose" => (LevelFilter::DEBUG, false),
        "trace" => (LevelFilter::TRACE, false),
        _ => (LevelFilter::INFO, true),
    }
}

/// Resolve the effective config from the layered sources (CONFIG.md): defaults ->
/// TOML file -> env vars -> CLI flags.
fn load_config(cli: &Cli) -> anyhow::Result<Config> {
    let file_overlay = if let Some(path) = &cli.config {
        ConfigOverlay::from_toml_file(path)
            .with_context(|| format!("loading config file {}", path.display()))?
    } else {
        // A conventional default path is checked but optional.
        let default_path = Path::new("/etc/ironcache/ironcache.toml");
        ConfigOverlay::from_toml_file(default_path)?
    };
    let env_overlay = ConfigOverlay::from_env().context("reading IRONCACHE_* env vars")?;

    // CLI flags overlay (highest of the startup layers).
    // The `--runtime` flag (PROD-10 / #28): parse the token to a `RuntimeBackend` here so a typo
    // is a clean boot error (mirrors the IRONCACHE_RUNTIME env parse), with the CLI as the highest
    // non-runtime-CONFIG layer. `None` (the no-flag default) leaves the lower layers showing through.
    let cli_runtime = match cli.runtime.as_deref() {
        Some(tok) => Some(ironcache_config::parse_runtime_backend(tok).ok_or_else(|| {
            anyhow::anyhow!("--runtime: not a runtime backend (expected tokio/io_uring): {tok}")
        })?),
        None => None,
    };
    let cli_overlay = ConfigOverlay {
        bind: cli.bind,
        port: cli.port,
        shards: cli.shards,
        runtime: cli_runtime,
        ..Default::default()
    };

    // resolve() hard-fails on a malformed/overflowing maxmemory (it no longer
    // silently degrades to 0 = unlimited), so a bad ceiling stops boot here.
    let cfg = Config::resolve(&[file_overlay, env_overlay, cli_overlay])
        .context("resolving effective config")?;
    cfg.validate().context("validating effective config")?;
    Ok(cfg)
}

fn cmd_server(cli: &Cli) -> anyhow::Result<()> {
    let mut cfg = load_config(cli)?;
    // FD BUDGET (#532, Redis `adjustOpenFilesLimit` parity): before wiring any shard,
    // raise the `RLIMIT_NOFILE` soft limit toward the hard limit where the kernel
    // allows, else CLAMP `maxclients` down to fit the file-descriptor budget with a
    // LOUD warning. This makes a low `ulimit -n` a clean bounded ceiling at boot
    // rather than an `EMFILE` mid-traffic. Boot / OS-seam code, outside ADR-0003.
    fd_budget::apply_fd_budget(&mut cfg);
    tracing::info!(
        version = cli::BUILD_VERSION,
        bind = %cfg.bind,
        port = cfg.port,
        shards = cfg.shards,
        "ironcache: binding"
    );

    // SNAPSHOT FORMAT-VERSION GUARD (#530): before we bind any port, check ONCE (at the node level)
    // whether the committed on-disk snapshot is a format version THIS binary can read. A dump written
    // by a NEWER binary (a downgrade / a failed-upgrade rollback) would otherwise be silently ignored
    // and the node would boot with an EMPTY keyspace -- then the next save could OVERWRITE the newer
    // dump, losing everything. `check_snapshot_loadable` emits a LOUD `tracing::error!` on such a
    // mismatch (so it is never silent), and here we FAIL CLOSED (refuse to boot) when the operator
    // opted in via `refuse_empty_start_on_version_mismatch`. With no `data_dir`, a genuinely absent
    // dump, or a loadable version this is a no-op (boot is byte-unchanged).
    if let Some(dir) = cfg.data_dir.as_deref() {
        if let Err(e) = ironcache_persist::check_snapshot_loadable(dir) {
            if cfg.refuse_empty_start_on_version_mismatch {
                return Err(anyhow::Error::new(e).context(
                    "refusing to boot: the on-disk snapshot has an unsupported format version and \
                     refuse_empty_start_on_version_mismatch is set (fail closed rather than start \
                     with an empty keyspace)",
                ));
            }
        }
    }

    // OUT-OF-BAND METRICS / HEALTH (OBSERVABILITY.md, #152). Enabled ONLY when `--metrics-addr`
    // is set: build the per-shard counter registry (sized to the shard count, adopted by each
    // shard at boot) and the liveness / readiness flags, boot the server with the registry
    // threaded through every shard's context, then spawn the HTTP endpoint. With NO `--metrics-addr`
    // the registry is `None`, the shards use a standalone counter cell, and NO listener is spawned
    // (byte-identical boot). The `live`/`ready` flags exist regardless (cheap atomics) but are only
    // read by the endpoint.
    let metrics_enabled = cli.metrics_addr.is_some();
    let registry = metrics_enabled.then(|| MetricsRegistry::new(cfg.shards));
    let live = Arc::new(AtomicBool::new(false));
    // Readiness gates on ACTUAL per-shard load-on-boot completion (OBSERVABILITY.md, #152): sized to
    // the shard count, it reports `/readyz` not-ready until EVERY shard's `load_shard_on_boot` has
    // returned. Threaded into the server boot below so each shard signals one unit when it finishes
    // loading; the flag is never flipped prematurely here. Built only when the endpoint is enabled.
    let ready = metrics_enabled.then(|| Arc::new(ReadyState::with_shards(cfg.shards)));

    let handles = serve::run_server_observed(&cfg, registry.clone(), ready.clone())
        .context("starting server")?;
    let set = handles.set;

    // The metrics endpoint reads the live boot handles (the raft status, persistence atomics, and
    // the runtime-config maxmemory). Spawn it AFTER the shards are up so a `/metrics` scrape sees a
    // live server. A bind failure here is a hard boot error (a misconfigured `--metrics-addr`
    // should fail fast, like the RESP listener), so propagate it before we mark the node live.
    // `registry` and `ready` are both `Some` exactly when `--metrics-addr` is set (built together
    // under `metrics_enabled`); the metrics state SHARES the SAME `ready` the shards signal into.
    if let (Some(metrics_addr), Some(registry), Some(ready)) =
        (cli.metrics_addr.as_ref(), registry, ready.as_ref())
    {
        let runtime = std::sync::Arc::clone(&handles.runtime);
        let state = MetricsState::new(
            registry,
            Arc::clone(&live),
            Arc::clone(ready),
            cfg.shards,
            Arc::new(move || runtime.maxmemory()),
            handles.raft.clone(),
            handles.persist.clone(),
            handles.topology.clone(),
            // The coordinator inbox, so `/metrics` samples the per-shard inbox-depth gauge (#556).
            Some(handles.inbox.clone()),
        );
        metrics_http::spawn_metrics_server(metrics_addr, state)
            .context("starting the metrics endpoint")?;
    }

    let flag = serve::install_shutdown(&set);
    // Boot is complete: the process is SERVING (liveness). Readiness is NOT flipped here: the
    // synchronous boot wiring only SPAWNS the shards, whose load-on-boot runs async afterward, so
    // each shard signals the `/readyz` countdown itself once its `load_shard_on_boot` returns (#152).
    // The raft-leader gate, when applicable, is evaluated live by `/readyz` from the raft handle.
    live.store(true, std::sync::atomic::Ordering::SeqCst);
    tracing::info!("ironcache: ready (PING -> +PONG). Ctrl-C to stop.");
    serve::wait_for_signal(&flag);
    tracing::info!("ironcache: shutting down");

    // GRACEFUL STOP (#139, SHUTDOWN.md). `wait_for_signal` has set the shutdown flag (and armed the
    // second-signal force-exit watcher). `shutdown_and_join` now drives the bounded per-shard drain
    // AND the SIGNAL-DRIVEN SAVE-ON-EXIT: when a save policy is configured, shard 0's drain loop
    // performs a final save (reusing the atomic SAVE path) then `exit(0)`s (the orchestrator
    // contract); the bootstrap awaits each shard's drain task (bounded by the drain grace) before
    // joining, so the join naturally waits for shard 0's save to commit rather than racing it. With
    // NO save policy (the default / NOSAVE posture) no save runs and this is the unchanged clean stop
    // -> the function returns Ok and `main` exits 0.
    if set.shutdown_and_join().is_err() {
        // A shard thread panicked; surface it as a non-zero exit rather than
        // pretending shutdown was clean.
        anyhow::bail!("one or more shard threads panicked during shutdown");
    }
    Ok(())
}

fn cmd_cli(host: &str, port: u16) -> anyhow::Result<()> {
    // PR-1: the interactive REPL is a documented WIP. We provide a minimal,
    // non-interactive smoke client so the mode is real: connect, PING, print.
    tracing::info!(host, port, "ironcache cli (WIP): connecting");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect((host, port))
            .await
            .with_context(|| format!("connecting to {host}:{port}"))?;
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        print!("{}", String::from_utf8_lossy(&buf[..n]));
        anyhow::Ok(())
    })?;
    Ok(())
}

fn run_cli_alias() -> anyhow::Result<()> {
    // Forward the remaining args (after argv[0]) to `cli`. For PR-1 we map the
    // common -h/-p flags; the full redis-cli flag surface lands with the REPL.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut host = "127.0.0.1".to_owned();
    let mut port = ironcache_config::DEFAULT_PORT;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" if i + 1 < args.len() => {
                host.clone_from(&args[i + 1]);
                i += 2;
            }
            "-p" | "--port" if i + 1 < args.len() => {
                // Surface a parse error instead of silently falling back to 6379.
                port = args[i + 1]
                    .parse()
                    .with_context(|| format!("invalid -p/--port value '{}'", args[i + 1]))?;
                i += 2;
            }
            _ => i += 1,
        }
    }
    cmd_cli(&host, port)
}

fn cmd_bench() {
    // Stub (CLI_BINARY.md): the benchmark harness is #8, a later PR.
    tracing::warn!("ironcache bench: not yet implemented (tracked by #8)");
}

fn cmd_check(cli: &Cli) -> anyhow::Result<()> {
    // Self-check: resolve and validate the effective config, report it. With no
    // data directory yet (PR-1 is ephemeral), the check is config-only.
    let cfg = load_config(cli)?;
    println!("ironcache check: configuration OK");
    println!("  bind        = {}:{}", cfg.bind, cfg.port);
    println!("  shards      = {}", cfg.shards);
    // The per-shard runtime backend (PROD-10 / #28). `io_uring` is honored only on a Linux build
    // with the `io_uring` feature + TLS off; otherwise the boot falls back to tokio (logged then).
    println!(
        "  runtime     = {}",
        match cfg.runtime {
            ironcache_config::RuntimeBackend::Tokio => "tokio",
            ironcache_config::RuntimeBackend::IoUring => "io_uring (Linux + feature + plaintext)",
        }
    );
    println!("  databases   = {}", cfg.databases);
    println!(
        "  maxmemory   = {} bytes{}",
        cfg.maxmemory,
        if cfg.maxmemory == 0 {
            " (unlimited)"
        } else {
            ""
        }
    );
    println!("  policy      = {}", cfg.maxmemory_policy);
    println!(
        "  requirepass = {}",
        if cfg.requirepass.is_some() {
            "set"
        } else {
            "unset"
        }
    );
    // Transport TLS posture (#105): report the client-listener mode + cert/key when on.
    match cfg.tls {
        ironcache_config::TlsMode::Off => println!("  tls         = off (plaintext)"),
        ironcache_config::TlsMode::On => {
            println!("  tls         = on (rustls, server-auth, client listener)");
            if let Some(cert) = &cfg.tls_cert_path {
                println!("  tls_cert    = {}", cert.display());
            }
            if let Some(key) = &cfg.tls_key_path {
                println!("  tls_key     = {}", key.display());
            }
        }
    }
    print_allocator_check();
    Ok(())
}

/// Report the live allocator configuration so `check` confirms the ADR-0006
/// jemalloc tuning (background purge thread on, sub-10s dirty decay) actually
/// landed. This also exercises the `tikv-jemalloc-ctl` `opt.*` mallctl path that
/// PR-3's `epoch` + `stats.allocated` accounting will build on.
#[cfg(not(target_env = "msvc"))]
fn print_allocator_check() {
    use tikv_jemalloc_ctl::{opt, raw};
    let bg = opt::background_thread::read()
        .map_or_else(|e| format!("unavailable ({e})"), |v| v.to_string());
    // dirty_decay_ms has no typed key in tikv-jemalloc-ctl; read it via the raw
    // mallctl path (it is an ssize_t). This is the same mallctl seam PR-3's
    // epoch + stats.allocated accounting uses.
    let decay = unsafe { raw::read::<libc::ssize_t>(b"opt.dirty_decay_ms\0") }
        .map_or_else(|e| format!("unavailable ({e})"), |v| format!("{v} ms"));
    println!("  allocator   = jemalloc (background_thread={bg}, dirty_decay_ms={decay})");
}

#[cfg(target_env = "msvc")]
fn print_allocator_check() {
    // Report "libc" (Redis's name for a system-malloc build), matching INFO's
    // `mem_allocator` (serve.rs GLOBAL_ALLOCATOR_NAME) so the same allocator has one
    // name across `check` and INFO.
    println!("  allocator   = libc (jemalloc not built on this target)");
}

fn cmd_config(cli: &Cli) -> anyhow::Result<()> {
    // Print the effective configuration (CLI_BINARY.md `config` reads the config).
    let cfg = load_config(cli)?;
    println!("# effective ironcache configuration");
    println!("bind = \"{}\"", cfg.bind);
    println!("port = {}", cfg.port);
    println!("shards = {}", cfg.shards);
    println!(
        "runtime = \"{}\"",
        match cfg.runtime {
            ironcache_config::RuntimeBackend::Tokio => "tokio",
            ironcache_config::RuntimeBackend::IoUring => "io_uring",
        }
    );
    println!("databases = {}", cfg.databases);
    println!("maxmemory = {}", cfg.maxmemory);
    println!("maxmemory-policy = \"{}\"", cfg.maxmemory_policy);
    println!("timeout = {}", cfg.timeout_secs);
    println!(
        "requirepass = {}",
        if cfg.requirepass.is_some() {
            "\"<set>\""
        } else {
            "\"\""
        }
    );
    Ok(())
}

/// `ironcache upgrade` (#387): the verified, data-safe, health-gated, auto-rolling-back binary
/// self-updater. Translates the clap [`cli::UpgradeArgs`] into the module's resolved
/// [`ironcache::upgrade::UpgradeArgs`] (reading the auth password from its FILE so it never lands in
/// argv/logs), then drives the orchestrator. A failure is surfaced as a nonzero exit via the
/// returned `anyhow::Result`; the structured per-step logging lives in the module.
fn cmd_upgrade(args: &cli::UpgradeArgs) -> anyhow::Result<()> {
    use ironcache::upgrade;

    // Read the optional auth password from its file (keeps the secret out of argv/logs).
    let auth = upgrade::read_auth_file(args.auth_file.as_deref())
        .context("reading the --auth-file password")?;

    // Resolve the binary source: a LOCAL `--binary` + `--sha256sums`, or a REMOTE `--from-url` +
    // `--sums-url` fetch (#394). `fetched` owns the downloaded temp dir; it is held for the whole
    // run so the extracted binary + derived manifest stay on disk until the swap completes.
    let (binary, sha256sums, fetched) = resolve_upgrade_source(args)?;

    let resolved = upgrade::UpgradeArgs {
        binary,
        sha256sums,
        target: args.target.clone(),
        unit: args.unit.clone(),
        readyz_addr: args.readyz_addr.clone(),
        resp_addr: args.resp_addr.clone(),
        auth,
        health_timeout: std::time::Duration::from_secs(args.health_timeout),
        no_rollback: args.no_rollback,
        yes: args.yes,
        allow_same: args.allow_same,
        no_freeze: args.no_freeze,
    };

    // FETCHED sources (--from-url / --to) hand the orchestrator a DERIVED per-binary manifest whose
    // AUTHENTICITY was already verified at fetch time when a key is pinned (fetch_release downloads
    // + verifies the real SHA256SUMS.minisig fail-closed), so they run integrity-only downstream.
    // The LOCAL --binary flow runs the full pinned-key selection (minisign over the operator's real
    // SHA256SUMS + .minisig when a key is committed).
    let outcome = if fetched.is_some() {
        upgrade::run_integrity_only(&resolved)
    } else {
        upgrade::run(&resolved)
    };
    match outcome {
        Ok(outcome) => {
            tracing::info!(
                version = %outcome.installed_version,
                previous = outcome.previous_version.as_deref().unwrap_or("(unknown)"),
                save_confirmed = outcome.save_confirmed,
                "ironcache upgrade: SUCCESS"
            );
            println!(
                "upgrade succeeded: now running {} (was {}){}",
                outcome.installed_version,
                outcome.previous_version.as_deref().unwrap_or("unknown"),
                if outcome.save_confirmed {
                    ""
                } else {
                    " [WARNING: no persisted snapshot was confirmed; in-memory data was not saved]"
                },
            );
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = %e, "ironcache upgrade: FAILED");
            // Propagate as a nonzero exit; anyhow prints the full error chain.
            Err(anyhow::Error::new(e).context("ironcache upgrade failed"))
        }
    }
}

/// Resolve the upgrade's binary source into a `(binary, sha256sums)` pair plus the optional fetch
/// guard. EXACTLY ONE source must be given (#394):
/// - LOCAL: `--binary <path>` + `--sha256sums <path>`.
/// - REMOTE explicit: `--from-url <url>` + `--sums-url <url>`.
/// - REMOTE GitHub: `--to <version|latest>` (+ optional `--repo`), which resolves this platform's
///   asset URLs itself.
///
/// The two remote paths download + verify the tarball, extract the binary, and return the
/// [`fetch::Fetched`] guard (its temp dir must outlive the run).
fn resolve_upgrade_source(
    args: &cli::UpgradeArgs,
) -> anyhow::Result<(
    std::path::PathBuf,
    std::path::PathBuf,
    Option<ironcache::upgrade::fetch::Fetched>,
)> {
    use ironcache::upgrade::fetch;

    // Exactly one of the three source modes must be selected.
    let n_sources = [
        args.binary.is_some(),
        args.from_url.is_some(),
        args.to.is_some(),
    ]
    .into_iter()
    .filter(|&m| m)
    .count();
    if n_sources == 0 {
        anyhow::bail!(
            "a binary source is required: `--binary <path> --sha256sums <path>` (local), \
             `--from-url <url> --sums-url <url>` (remote), or `--to <version|latest>` (GitHub, #394)"
        );
    }
    if n_sources > 1 {
        anyhow::bail!(
            "the sources --binary, --from-url, and --to are mutually exclusive; choose exactly ONE"
        );
    }

    // REMOTE GitHub: `--to <version|latest>` -> resolve this platform's asset URLs, then fetch.
    if let Some(spec) = args.to.as_deref() {
        let repo = args.repo.as_deref().unwrap_or(fetch::DEFAULT_UPGRADE_REPO);
        let plat = fetch::target_plat().ok_or_else(|| {
            anyhow::anyhow!(
                "this platform has no published release asset (the release ships Linux musl/glibc on \
                 amd64/arm64); use --from-url or --binary instead"
            )
        })?;
        let bounds = fetch::FetchBounds::default();
        let tag = if spec.eq_ignore_ascii_case("latest") {
            println!("upgrade: resolving the latest release of {repo}");
            fetch::resolve_latest_tag(repo, bounds).context("resolving the latest release tag")?
        } else {
            spec.to_owned()
        };
        let version = fetch::version_from_tag(&tag);
        let (tarball_url, sums_url) = fetch::github_release_urls(repo, &tag, version, plat);
        println!("upgrade: fetching {tarball_url}");
        let fetched = fetch::fetch_release(&tarball_url, &sums_url, bounds)
            .context("fetching the release from GitHub")?;
        return Ok((
            fetched.binary.clone(),
            fetched.sha256sums.clone(),
            Some(fetched),
        ));
    }

    // REMOTE explicit: `--from-url <url>` + `--sums-url <url>`.
    if let Some(url) = args.from_url.as_deref() {
        let sums_url = args.sums_url.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "--from-url requires --sums-url (the SHA256SUMS URL that vouches for the tarball)"
            )
        })?;
        if args.sha256sums.is_some() {
            anyhow::bail!(
                "--sha256sums is for the local --binary source; use --sums-url with --from-url"
            );
        }
        println!("upgrade: fetching {url}");
        let fetched = fetch::fetch_release(url, sums_url, fetch::FetchBounds::default())
            .context("fetching the release over HTTPS")?;
        println!(
            "upgrade: fetched + verified the tarball; installing the extracted binary {}",
            fetched.binary.display()
        );
        return Ok((
            fetched.binary.clone(),
            fetched.sha256sums.clone(),
            Some(fetched),
        ));
    }

    // LOCAL: `--binary <path>` + `--sha256sums <path>`.
    let binary = args
        .binary
        .clone()
        .expect("exactly one source selected; binary is it");
    let sums = args
        .sha256sums
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--binary requires --sha256sums"))?;
    Ok((binary, sums, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::level_filters::LevelFilter;

    /// Parse an `ironcache upgrade ...` argv into its [`cli::UpgradeArgs`] (via the real clap surface).
    fn parse_upgrade(argv: &[&str]) -> cli::UpgradeArgs {
        use clap::Parser as _;
        let full: Vec<&str> = std::iter::once("ironcache")
            .chain(std::iter::once("upgrade"))
            .chain(argv.iter().copied())
            .collect();
        match cli::Cli::try_parse_from(full)
            .expect("upgrade args parse")
            .command
        {
            Some(cli::Command::Upgrade(a)) => a,
            other => panic!("expected the upgrade subcommand, got {other:?}"),
        }
    }

    #[test]
    fn resolve_source_local_returns_the_paths() {
        let args = parse_upgrade(&["--binary", "/tmp/ic", "--sha256sums", "/tmp/SUMS"]);
        let (bin, sums, fetched) = resolve_upgrade_source(&args).expect("local mode resolves");
        assert_eq!(bin, std::path::PathBuf::from("/tmp/ic"));
        assert_eq!(sums, std::path::PathBuf::from("/tmp/SUMS"));
        assert!(fetched.is_none(), "local mode does not fetch");
    }

    #[test]
    fn resolve_source_binary_without_sums_errors() {
        let args = parse_upgrade(&["--binary", "/tmp/ic"]);
        let err = resolve_upgrade_source(&args).expect_err("--binary needs --sha256sums");
        assert!(
            err.to_string().contains("--binary requires --sha256sums"),
            "{err}"
        );
    }

    #[test]
    fn resolve_source_from_url_without_sums_url_errors() {
        let args = parse_upgrade(&["--from-url", "https://x/a.tar.gz"]);
        let err = resolve_upgrade_source(&args).expect_err("--from-url needs --sums-url");
        assert!(
            err.to_string().contains("--from-url requires --sums-url"),
            "{err}"
        );
    }

    #[test]
    fn resolve_source_both_modes_is_rejected() {
        let args = parse_upgrade(&[
            "--binary",
            "/tmp/ic",
            "--sha256sums",
            "/tmp/SUMS",
            "--from-url",
            "https://x/a.tar.gz",
            "--sums-url",
            "https://x/SHA256SUMS",
        ]);
        let err = resolve_upgrade_source(&args).expect_err("cannot mix local + remote");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    #[test]
    fn resolve_source_no_source_is_rejected() {
        let args = parse_upgrade(&[]);
        let err = resolve_upgrade_source(&args).expect_err("a source is required");
        assert!(
            err.to_string().contains("a binary source is required"),
            "{err}"
        );
    }

    #[test]
    fn resolve_source_to_conflicts_with_other_sources() {
        // --to is mutually exclusive with --binary (checked before any platform/network work).
        let args = parse_upgrade(&[
            "--to",
            "latest",
            "--binary",
            "/tmp/ic",
            "--sha256sums",
            "/tmp/S",
        ]);
        let err = resolve_upgrade_source(&args).expect_err("--to + --binary is rejected");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
        // And with --from-url.
        let args2 = parse_upgrade(&[
            "--to",
            "2026.0701.1",
            "--from-url",
            "https://x/a.tgz",
            "--sums-url",
            "https://x/S",
        ]);
        let err2 = resolve_upgrade_source(&args2).expect_err("--to + --from-url is rejected");
        assert!(err2.to_string().contains("mutually exclusive"), "{err2}");
    }

    /// On a platform with no published release asset (e.g. macOS), `--to` fails at the platform check
    /// BEFORE any network access. On Linux CI `target_plat` is `Some`, so this path would try to reach
    /// GitHub; gate the assertion to non-Linux so it is deterministic (the happy path is covered by
    /// the fetch unit tests).
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn resolve_source_to_on_unsupported_platform_errors_before_network() {
        let args = parse_upgrade(&["--to", "latest"]);
        let err = resolve_upgrade_source(&args).expect_err("no asset for this platform");
        assert!(
            err.to_string().contains("no published release asset"),
            "{err}"
        );
    }

    #[test]
    fn parse_log_level_maps_the_vocabulary() {
        assert_eq!(parse_log_level("error"), (LevelFilter::ERROR, false));
        assert_eq!(parse_log_level("warn"), (LevelFilter::WARN, false));
        assert_eq!(parse_log_level("warning"), (LevelFilter::WARN, false));
        assert_eq!(parse_log_level("info"), (LevelFilter::INFO, false));
        assert_eq!(parse_log_level("debug"), (LevelFilter::DEBUG, false));
        assert_eq!(parse_log_level("trace"), (LevelFilter::TRACE, false));
        // Case-insensitive.
        assert_eq!(parse_log_level("INFO"), (LevelFilter::INFO, false));
        assert_eq!(parse_log_level("Debug"), (LevelFilter::DEBUG, false));
    }

    #[test]
    fn parse_log_level_unknown_falls_back_to_info() {
        let (level, unknown) = parse_log_level("loud");
        assert_eq!(level, LevelFilter::INFO);
        assert!(unknown, "an unrecognized level must flag the fallback");
    }

    #[test]
    fn parse_log_level_filters_debug_at_info() {
        // A debug-line is SUPPRESSED at info (info is less verbose) and VISIBLE at debug. We assert
        // the relation on the LevelFilter the subscriber installs from the flag: DEBUG > INFO, so a
        // DEBUG event passes the debug filter but not the info filter.
        let (info, _) = parse_log_level("info");
        let (debug, _) = parse_log_level("debug");
        assert!(
            tracing::Level::DEBUG <= debug,
            "a debug event must pass the debug filter"
        );
        assert!(
            tracing::Level::DEBUG > info,
            "a debug event must be suppressed at info"
        );
    }

    #[test]
    fn install_tracing_does_not_panic() {
        // Installing the subscriber from the flag must not panic (a second install in the same
        // process is ignored). This also exercises the unknown-level branch.
        install_tracing("info");
        install_tracing("nonsense");
    }
}
