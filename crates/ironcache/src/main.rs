// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache binary entry point (CLI_BINARY.md, ADR-0020).
//!
//! One static binary, six modes. The `server` mode (the default) boots the
//! shared-nothing thread-per-core runtime and serves Tier-0 RESP. The default
//! global allocator is jemalloc (ADR-0006). Signal handling for graceful shutdown
//! lives here in the binary, never in the library crates, so the determinism
//! boundary (ADR-0003) holds.

mod cli;
mod serve;

use anyhow::Context as _;
use clap::Parser;
use cli::{Cli, Command};
use ironcache_config::{Config, ConfigOverlay};
use std::path::Path;

// jemalloc as the global allocator (ADR-0006). Not available on MSVC; the static
// release targets (musl, macos) all have it.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    // redis-cli argv[0] alias: forward to `cli` (ADR-0020). If invoked as
    // `redis-cli`, we synthesize a `cli` invocation from the remaining args.
    let argv0 = std::env::args().next().unwrap_or_default();
    if cli::invoked_as_redis_cli(&argv0) {
        return run_cli_alias();
    }

    let cli = Cli::parse();
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
        Some(Command::Upgrade) => {
            cmd_upgrade();
            Ok(())
        }
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
    let cli_overlay = ConfigOverlay {
        bind: cli.bind,
        port: cli.port,
        shards: cli.shards,
        ..Default::default()
    };

    let cfg = Config::resolve(&[file_overlay, env_overlay, cli_overlay]);
    cfg.validate().context("validating effective config")?;
    Ok(cfg)
}

fn cmd_server(cli: &Cli) -> anyhow::Result<()> {
    let cfg = load_config(cli)?;
    eprintln!(
        "ironcache {}: binding {}:{} across {} shard(s)",
        env!("CARGO_PKG_VERSION"),
        cfg.bind,
        cfg.port,
        cfg.shards
    );
    let set = serve::run_server(&cfg).context("starting server")?;
    let flag = serve::install_shutdown(&set);
    eprintln!("ironcache: ready (PING -> +PONG). Ctrl-C to stop.");
    serve::wait_for_signal(&flag);
    eprintln!("ironcache: shutting down");
    set.shutdown_and_join();
    Ok(())
}

fn cmd_cli(host: &str, port: u16) -> anyhow::Result<()> {
    // PR-1: the interactive REPL is a documented WIP. We provide a minimal,
    // non-interactive smoke client so the mode is real: connect, PING, print.
    eprintln!("ironcache cli (WIP): connecting to {host}:{port}");
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
                port = args[i + 1].parse().unwrap_or(port);
                i += 2;
            }
            _ => i += 1,
        }
    }
    cmd_cli(&host, port)
}

fn cmd_bench() {
    // Stub (CLI_BINARY.md): the benchmark harness is #8, a later PR.
    eprintln!("ironcache bench: not yet implemented (tracked by #8)");
}

fn cmd_check(cli: &Cli) -> anyhow::Result<()> {
    // Self-check: resolve and validate the effective config, report it. With no
    // data directory yet (PR-1 is ephemeral), the check is config-only.
    let cfg = load_config(cli)?;
    println!("ironcache check: configuration OK");
    println!("  bind        = {}:{}", cfg.bind, cfg.port);
    println!("  shards      = {}", cfg.shards);
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
    println!(
        "  requirepass = {}",
        if cfg.requirepass.is_some() {
            "set"
        } else {
            "unset"
        }
    );
    Ok(())
}

fn cmd_config(cli: &Cli) -> anyhow::Result<()> {
    // Print the effective configuration (CLI_BINARY.md `config` reads the config).
    let cfg = load_config(cli)?;
    println!("# effective ironcache configuration");
    println!("bind = \"{}\"", cfg.bind);
    println!("port = {}", cfg.port);
    println!("shards = {}", cfg.shards);
    println!("databases = {}", cfg.databases);
    println!("maxmemory = {}", cfg.maxmemory);
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

fn cmd_upgrade() {
    // Stub (CLI_BINARY.md / #83): verified self-update with rollback.
    eprintln!("ironcache upgrade: not yet implemented (tracked by #83)");
}
