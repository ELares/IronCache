// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache binary entry point (CLI_BINARY.md, ADR-0020).
//!
//! One static binary, six modes. The `server` mode (the default) boots the
//! shared-nothing thread-per-core runtime and serves Tier-0 RESP. The default
//! global allocator is jemalloc (ADR-0006). Signal handling for graceful shutdown
//! lives here in the binary, never in the library crates, so the determinism
//! boundary (ADR-0003) holds.

mod cli;

use anyhow::Context as _;
use clap::Parser;
use cli::{Cli, Command};
// The server wiring lives in the crate's library half (`src/lib.rs`) so integration
// tests can boot the real `run_server`; the binary consumes the same modules here.
use ironcache::serve;
use ironcache_config::{Config, ConfigOverlay};
use std::path::Path;

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

    // resolve() hard-fails on a malformed/overflowing maxmemory (it no longer
    // silently degrades to 0 = unlimited), so a bad ceiling stops boot here.
    let cfg = Config::resolve(&[file_overlay, env_overlay, cli_overlay])
        .context("resolving effective config")?;
    cfg.validate().context("validating effective config")?;
    Ok(cfg)
}

fn cmd_server(cli: &Cli) -> anyhow::Result<()> {
    let cfg = load_config(cli)?;
    eprintln!(
        "ironcache {}: binding {}:{} across {} shard(s)",
        cli::BUILD_VERSION,
        cfg.bind,
        cfg.port,
        cfg.shards
    );
    let set = serve::run_server(&cfg).context("starting server")?;
    let flag = serve::install_shutdown(&set);
    eprintln!("ironcache: ready (PING -> +PONG). Ctrl-C to stop.");
    serve::wait_for_signal(&flag);
    eprintln!("ironcache: shutting down");

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

fn cmd_upgrade() {
    // Stub (CLI_BINARY.md / #83): verified self-update with rollback.
    eprintln!("ironcache upgrade: not yet implemented (tracked by #83)");
}
