// SPDX-License-Identifier: MIT OR Apache-2.0
//! IronCache binary entry point (CLI_BINARY.md, ADR-0020).
//!
//! One static binary, six modes. The `server` mode (the default) boots the
//! shared-nothing thread-per-core runtime and serves Tier-0 RESP. The default
//! global allocator is jemalloc (ADR-0006). Signal handling for graceful shutdown
//! lives here in the binary, never in the library crates, so the determinism
//! boundary (ADR-0003) holds.

mod cli;
mod cluster_bus;
mod fd_budget;
mod sockact_log;

use anyhow::Context as _;
use clap::Parser;
use cli::{Cli, Command};
// The server wiring lives in the crate's library half (`src/lib.rs`) so integration
// tests can boot the real `run_server`; the binary consumes the same modules here.
use ironcache::metrics_http::{self, MetricsState, ReadyState};
use ironcache::serve;
use ironcache::upgrade::cutover_coord::CutoverAction;
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
// TRANSPARENT HUGE PAGES (#512). On Linux, the default-off `hugepages` Cargo
// feature appends `thp:always,metadata_thp:auto`, so jemalloc backs its extents
// (the per-shard hashbrown store tables AND the value blobs, all of which flow
// through the global allocator) and its own arena metadata with 2 MiB transparent
// huge pages via madvise(MADV_HUGEPAGE). The `-r 1M` random-key hot path touches a
// random table bucket plus a random value per GET, so with 4 KiB pages the TLB
// thrashes; 2 MiB pages cut the TLB-miss rate for the same coverage. This is a
// process-wide memory HINT, not an engine decision, so it stays clear of the
// ADR-0003 determinism boundary (no clock/RNG). It is compiled in ONLY on Linux
// (jemalloc builds THP support on Linux and nowhere else): on macOS and other
// targets the string stays THP-free, so there is no jemalloc "Invalid conf pair"
// warning and the feature is inert. See docs/design/CONFIG.md ("Transparent huge
// pages") for the RSS/latency tradeoff, why the default is OFF, and the runtime
// override.
//
// TUNABILITY (the tunability tenet: env-dependent tradeoffs are config knobs with a
// safe default). THP is a behavior tradeoff, so it is a
// knob with a safe default rather than a baked-in choice. It is DEFAULT-OFF because
// `thp:always` can raise RSS (2 MiB allocation granularity) and, on some kernels,
// add khugepaged compaction latency spikes; keeping it off preserves an honest RSS
// figure for the maxmemory ceiling (ADR-0006). The build-time knob is the
// `hugepages` feature (flips the compiled default). The RUNTIME knob, which works on
// ANY shipped binary with no rebuild, is jemalloc's own env override
// `_RJEM_MALLOC_CONF=thp:always` (or `thp:never`): jemalloc layers it on top of this
// static string, overriding only the `thp` key, so `background_thread`/`dirty_decay_ms`
// stay put.
//
// tikv-jemalloc-sys builds jemalloc with the `_rjem_` prefix on our targets
// (musl/macos; macOS forces prefixing), so the symbol downstream is
// `_rjem_malloc_conf`. Same cfg-gate as the allocator so MSVC (which has no
// jemalloc here) is unaffected.

// The Linux + `hugepages` string: background purge + decay, PLUS THP on the
// jemalloc extents and metadata (the #512 huge-page path).
#[cfg(all(not(target_env = "msvc"), target_os = "linux", feature = "hugepages"))]
const MALLOC_CONF_CSTR: &core::ffi::CStr =
    c"background_thread:true,dirty_decay_ms:5000,thp:always,metadata_thp:auto";

// Every other non-MSVC target/feature combination: the THP-free default. Emitting no
// `thp:` token keeps non-Linux jemalloc (which has no THP support compiled in)
// warning-free, and keeps THP off by default even on Linux (opt-in per the tradeoff).
#[cfg(all(
    not(target_env = "msvc"),
    not(all(target_os = "linux", feature = "hugepages"))
))]
const MALLOC_CONF_CSTR: &core::ffi::CStr = c"background_thread:true,dirty_decay_ms:5000";

#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    // `MALLOC_CONF_CSTR` is a NUL-terminated C string literal; jemalloc reads this
    // pointer at init. Cast to the `libc::c_char` the exported symbol type requires.
    &*MALLOC_CONF_CSTR.as_ptr().cast::<libc::c_char>()
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
        Some(Command::Upgrade(args)) => cmd_upgrade(&cli, args),
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
    // Read the env layer FIRST: it carries the #527 config-rollback escape hatch
    // (`IRONCACHE_IGNORE_UNKNOWN_CONFIG_KEYS`) that, together with the `--ignore-unknown-config-keys`
    // CLI flag, must be resolved BEFORE the config FILE is parsed -- the hatch decides whether an
    // unknown FILE key is a loud WARN (a rollback past a forward-incompatible key) or a hard boot
    // failure (the strict default). The file's OWN `ignore_unknown_config_keys = true` also enables
    // it; from_toml_file_lenient ORs the two, so the switch works set at ANY layer.
    let env_overlay = ConfigOverlay::from_env().context("reading IRONCACHE_* env vars")?;
    let bootstrap_ignore_unknown =
        cli.ignore_unknown_config_keys || env_overlay.ignore_unknown_config_keys == Some(true);
    let file_overlay = if let Some(path) = &cli.config {
        ConfigOverlay::from_toml_file_lenient(path, bootstrap_ignore_unknown)
            .with_context(|| format!("loading config file {}", path.display()))?
    } else {
        // A conventional default path is checked but optional.
        let default_path = Path::new("/etc/ironcache/ironcache.toml");
        ConfigOverlay::from_toml_file_lenient(default_path, bootstrap_ignore_unknown)?
    };

    // CLI flags overlay (highest of the startup layers).
    // The `--runtime` flag (PROD-10 / #28): parse the token to a `RuntimeBackend` here so a typo
    // is a clean boot error (mirrors the IRONCACHE_RUNTIME env parse), with the CLI as the highest
    // non-runtime-CONFIG layer. `None` (the no-flag default) leaves the lower layers showing through.
    let cli_runtime = match cli.runtime.as_deref() {
        Some(tok) => Some(ironcache_config::parse_runtime_backend(tok).ok_or_else(|| {
            anyhow::anyhow!(
                "--runtime: not a runtime backend (expected tokio / io_uring / io_uring_raw): {tok}"
            )
        })?),
        None => None,
    };
    let cli_overlay = ConfigOverlay {
        bind: cli.bind,
        port: cli.port,
        shards: cli.shards,
        runtime: cli_runtime,
        // The dedicated persist core (#589): the raw string folds through; it is parsed + validated
        // on the resolved value in `Config::validate`, so a bad `--persist-cpu` fails boot here.
        persist_cpu: cli.persist_cpu.clone(),
        // The #527 escape hatch also flows through as an overlay field for completeness (CONFIG GET
        // parity + a single precedence story); it was ALREADY consumed above to parse the FILE, and
        // it is a bootstrap-only knob so apply_to never folds it onto the live Config.
        ignore_unknown_config_keys: cli.ignore_unknown_config_keys.then_some(true),
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
    // SOCKET ACTIVATION (#389): capture the systemd `LISTEN_*` environment ONCE, then UNSET it (the
    // `sd_listen_fds` convention). This is the FIRST action of the server boot, before any config
    // load or thread spawn, so the unset is sound (no other thread reads the environment yet) and the
    // captured snapshot feeds every later consumer -- the loud adopt-vs-fallback log below and the
    // RESP listener adoption in `run_shards`. Clearing the vars stops a later-exec'd child from
    // re-adopting fds meant for THIS pid. A no-op when not socket-activated (env absent). Boot/OS
    // seam, outside the ADR-0003 determinism boundary.
    ironcache_runtime::listen_fds::prime_from_env_and_unset();

    let mut cfg = load_config(cli)?;
    // FD BUDGET (#532, Redis `adjustOpenFilesLimit` parity): before wiring any shard,
    // raise the `RLIMIT_NOFILE` soft limit toward the hard limit where the kernel
    // allows, else CLAMP `maxclients` down to fit the file-descriptor budget with a
    // LOUD warning. This makes a low `ulimit -n` a clean bounded ceiling at boot
    // rather than an `EMFILE` mid-traffic. Boot / OS-seam code, outside ADR-0003.
    fd_budget::apply_fd_budget(&mut cfg);
    // K8s/k3s OOMKill GUARD (P0-1): if `maxmemory` was left unset and this process runs under a
    // finite cgroup MEMORY limit, cap it at a fraction of the limit so the cache EVICTS under
    // pressure instead of being OOMKilled (exit 137 -> data loss). No-op when `maxmemory` is set
    // explicitly or there is no cgroup limit (non-container / non-Linux). Boot / OS-seam, outside
    // ADR-0003 (like `apply_fd_budget` above). Logged at INFO when it fires.
    cfg.apply_cgroup_memory_guard();
    // CLUSTER-BUS SECURITY (#557): if a clustered mode is configured WITHOUT a cluster_secret and
    // with cluster_tls off, the inter-node RAFTMSG bus + replication stream run plaintext and
    // unauthenticated, so any peer reaching the bus port could join consensus or siphon the
    // keyspace. Emit a LOUD boot warning naming the exposure + how to secure it (a no-op for the
    // default standalone node and for any authenticated posture). Boot / OS-seam, outside ADR-0003.
    cluster_bus::warn_if_unauthenticated(&cfg);
    tracing::info!(
        version = cli::BUILD_VERSION,
        bind = %cfg.bind,
        port = cfg.port,
        shards = cfg.shards,
        "ironcache: binding"
    );
    // SOCKET ACTIVATION (#562, #389): state LOUDLY whether this boot ADOPTED the listening fd(s)
    // systemd passed (the listen queue then survives an upgrade restart with no connection-refused
    // window) or FELL BACK to self-binding, and why (e.g. no LISTEN_FDS, or a LISTEN_PID mismatch).
    // Without this an operator cannot tell from the logs which path a socket-activated upgrade took.
    // Boot / OS-seam, outside ADR-0003 (the classification is the pure runtime-crate `classify`).
    sockact_log::log_boot_socket_activation();

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

    // OUT-OF-BAND METRICS / HEALTH (OBSERVABILITY.md, #152; DEFAULT-ON #555). Resolve the effective
    // ops-endpoint bind (the tunability principle): with NO `--metrics-addr` this is the LOCALHOST
    // default (`127.0.0.1:9091`), so `/metrics` + the k8s probes are scrapable out of the box without
    // exposing the port publicly; an explicit `host:port` overrides it, and `--metrics-addr off`
    // disables the endpoint entirely (`metrics_addr` is then `None`). When enabled we build the
    // per-shard counter registry (sized to the shard count, adopted by each shard at boot) and the
    // liveness / readiness flags, boot the server with the registry threaded through every shard's
    // context, then spawn the HTTP endpoint. When DISABLED the registry is `None`, the shards use a
    // standalone counter cell, and NO listener is spawned (byte-identical to the prior default-off
    // boot). The `live`/`ready` flags exist regardless (cheap atomics) but are only read by the
    // endpoint.
    let metrics_addr = cli::effective_metrics_addr(cli.metrics_addr.as_deref());
    let metrics_enabled = metrics_addr.is_some();
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
    // `registry` and `ready` are both `Some` exactly when the endpoint is enabled (built together
    // under `metrics_enabled`); the metrics state SHARES the SAME `ready` the shards signal into.
    if let (Some(metrics_addr), Some(registry), Some(ready)) =
        (metrics_addr, registry, ready.as_ref())
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

    // HOT TLS CERT RELOAD (#563): when TLS is on, arm the SIGHUP handler over the client-listener
    // reload handle so an operator can rotate a soon-to-expire cert by replacing the configured
    // cert/key files and sending SIGHUP -- no restart, no dropped connections. A bad replacement is
    // logged and rejected (the previous cert stays live). `None` on a plaintext boot (no handler).
    if let Some(tls_reload) = handles.tls_reload {
        serve::spawn_tls_reload_on_sighup(tls_reload);
    }

    let flag = serve::install_shutdown(&set);
    // Boot is complete: the process is SERVING (liveness). Readiness is NOT flipped here: the
    // synchronous boot wiring only SPAWNS the shards, whose load-on-boot runs async afterward, so
    // each shard signals the `/readyz` countdown itself once its `load_shard_on_boot` returns (#152).
    // The raft-leader gate, when applicable, is evaluated live by `/readyz` from the raft handle.
    live.store(true, std::sync::atomic::Ordering::SeqCst);
    tracing::info!(
        "ironcache: ready (PING -> +PONG). Ctrl-C to stop. SIGUSR1 -> streamed cutover."
    );

    // THE SIGNAL LOOP (#139 shutdown + #638 SIGUSR1 streamed live cutover). `wait_for_signal` blocks
    // until a signal arrives and reports whether it was a SHUTDOWN (SIGINT/SIGTERM: it has ALREADY set
    // the flag + armed the second-signal force-exit watcher, byte-identical to before) or a CUTOVER
    // (SIGUSR1). A cutover runs the in-server host BEFORE any shutdown flag; on a COMMIT it sets the
    // flag so the normal graceful drain runs (shard 0 exits(0)); on a non-commit it resumes serving
    // and loops back to wait for the next signal (the OLD keeps full authority).
    loop {
        match serve::wait_for_signal(&flag) {
            serve::SignalOutcome::Shutdown => break,
            serve::SignalOutcome::Cutover => {
                match drive_cutover(&cfg, &set, &handles.cutover_control) {
                    CutoverAction::SetShutdownFlag => {
                        // The cutover COMMITTED. Mark this as a cutover HANDOFF exit (#638) BEFORE the
                        // shutdown flag: the drain paths then use the SHORT `CUTOVER_DRAIN_GRACE` (the OLD
                        // closes its already-quiesced client connections promptly so a client retrying
                        // `-LOADING` reconnects to the NEW immediately -- sub-second stall), and shard 0
                        // SKIPS the redundant save-on-exit (the NEW already durably promoted state@E). A
                        // committed cutover is a handoff, not a leisurely shutdown; SIGINT/SIGTERM never
                        // sets this flag, so their full-`DRAIN_GRACE` graceful stop is byte-unchanged.
                        ironcache_runtime::bootstrap::mark_cutover_exit();
                        flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                    CutoverAction::Resume => {
                        tracing::warn!(
                            "ironcache: streamed cutover did not commit; resuming service (the OLD keeps \
                         serving). Waiting for the next signal."
                        );
                    }
                }
            }
        }
    }
    tracing::info!("ironcache: shutting down");

    // GRACEFUL STOP (#139, SHUTDOWN.md). The shutdown flag is set (by `wait_for_signal` on
    // SIGINT/SIGTERM, or above on a committed cutover). `shutdown_and_join` now drives the bounded
    // per-shard drain AND the SIGNAL-DRIVEN SAVE-ON-EXIT: when a save policy is configured, shard 0's
    // drain loop performs a final save (reusing the atomic SAVE path) then `exit(0)`s (the
    // orchestrator contract); the bootstrap awaits each shard's drain task (bounded by the drain
    // grace) before joining, so the join naturally waits for shard 0's save to commit rather than
    // racing it. With NO save policy (the default / NOSAVE posture) no save runs and this is the
    // unchanged clean stop -> the function returns Ok and `main` exits 0.
    if set.shutdown_and_join().is_err() {
        // A shard thread panicked; surface it as a non-zero exit rather than
        // pretending shutdown was clean.
        anyhow::bail!("one or more shard threads panicked during shutdown");
    }
    Ok(())
}

/// Drive the in-server STREAMED LIVE CUTOVER host (#638 slice-3), returning the lifecycle
/// [`CutoverAction`] `main` takes. Selected when a SIGUSR1 arrives: spawn the receiver sibling
/// (inheriting the client listener fd for the no-RST handoff), deliver each shard its
/// [`CutoverStart`](ironcache::upgrade::cutover_coord::CutoverStart) over its dedicated control
/// channel, and drive the cross-shard barrier to a commit-or-abort decision.
///
/// FAIL-SAFE toward keep-serving: NO `handoff_socket` configured, a runtime-build failure, or ANY
/// host error yields [`CutoverAction::Resume`] (the OLD keeps serving); only a clean COMMIT yields
/// [`CutoverAction::SetShutdownFlag`]. Crash-simple: an error NEVER exits.
#[cfg(unix)]
fn drive_cutover(
    cfg: &Config,
    set: &ironcache_runtime::bootstrap::ShardSet,
    control: &[tokio::sync::mpsc::Sender<ironcache::upgrade::cutover_coord::CutoverStart>],
) -> CutoverAction {
    use ironcache::upgrade::drive::HandoffPlan;

    // The OPT-IN gate: with NO `handoff_socket` there is no streamed cutover to run -- log + resume.
    let Some(plan) = HandoffPlan::from_config(cfg) else {
        tracing::warn!(
            "ironcache: SIGUSR1 cutover requested but no handoff_socket is configured; ignoring \
             (the server keeps serving)"
        );
        return CutoverAction::Resume;
    };
    // The host coordinator drives async on its OWN current-thread runtime on this (idle) main thread.
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        tracing::error!("ironcache: could not build the cutover host runtime; resuming service");
        return CutoverAction::Resume;
    };
    let result = rt.block_on(run_cutover_host(&plan, set, control));
    match ironcache::upgrade::cutover_coord::cutover_action(&result) {
        CutoverAction::SetShutdownFlag => {
            tracing::info!(
                "ironcache: streamed cutover COMMITTED; the new sibling now serves on the inherited \
                 listener. Draining in-flight connections and exiting."
            );
            CutoverAction::SetShutdownFlag
        }
        CutoverAction::Resume => {
            match &result {
                Ok(_) => {
                    tracing::warn!("ironcache: streamed cutover aborted; the OLD keeps serving");
                }
                Err(e) => tracing::error!(
                    error = %e,
                    "ironcache: streamed cutover ended without a confirmed commit (degraded standby, \
                     W3); NOT exiting. Operator recovery: restart the OLD or the NEW."
                ),
            }
            CutoverAction::Resume
        }
    }
}

/// The non-unix stub: the streamed handoff rides an AF_UNIX socket, so there is no cutover off unix
/// (and `wait_for_signal` never returns `Cutover` there). Keeps the `main` seam uniform.
#[cfg(not(unix))]
fn drive_cutover(
    _cfg: &Config,
    _set: &ironcache_runtime::bootstrap::ShardSet,
    _control: &[tokio::sync::mpsc::Sender<ironcache::upgrade::cutover_coord::CutoverStart>],
) -> CutoverAction {
    CutoverAction::Resume
}

/// The #638 slice-3 in-server cutover HOST drive (async, on the main thread's own runtime).
///
/// 1. Bind every per-shard handoff listener on THIS (host) runtime BEFORE spawning the sibling, so
///    the sibling's per-shard connect (which retries) always finds a bound socket.
/// 2. Spawn the receiver sibling, inheriting the SINGLE client listener fd (no-RST inherited
///    listener). The trigger is gated to the single-listener default here; the shard-owner (#517)
///    N-listener fd-array inherit is a documented follow-up.
/// 3. Build the `Send` [`CutoverCoord`](ironcache::upgrade::cutover_coord::CutoverCoord), accept each
///    shard's connection, convert it to a reactor-free `std` stream, and deliver each shard its
///    [`CutoverStart`](ironcache::upgrade::cutover_coord::CutoverStart) over its dedicated control
///    channel (the drain loop's 3rd arm re-adopts the stream + spawns the per-shard task).
/// 4. Drive the cross-shard barrier to a commit-or-abort decision.
///
/// CRASH-SIMPLE: every fallible step maps to an `Err`, never an unwrap; the caller treats any error
/// as keep-serving (never exit).
///
/// # Errors
/// A [`HandoffError`](ironcache::upgrade::stream::HandoffError) on any bind/spawn/accept/deliver
/// failure, or when the barrier could not confirm every shard `Served` (W3 degraded standby).
#[cfg(unix)]
async fn run_cutover_host(
    plan: &ironcache::upgrade::drive::HandoffPlan,
    set: &ironcache_runtime::bootstrap::ShardSet,
    control: &[tokio::sync::mpsc::Sender<ironcache::upgrade::cutover_coord::CutoverStart>],
) -> Result<
    ironcache::upgrade::orchestrator::SenderDecision,
    ironcache::upgrade::stream::HandoffError,
> {
    use ironcache::upgrade::cutover_coord::{CutoverStart, drive_sender_cutover_host, new_cutover};
    use ironcache::upgrade::drive::bind_handoff_listener_for_shard;
    use ironcache::upgrade::orchestrator::spawn_receiver_sibling;
    use ironcache::upgrade::stream::HandoffError;

    let n = control.len();

    // Step 1: bind every per-shard handoff listener up front (fail fast; no half-bound state).
    let mut listeners = Vec::with_capacity(n);
    for i in 0..n {
        listeners.push(bind_handoff_listener_for_shard(&plan.socket, i)?);
    }

    // Step 2: spawn the receiver sibling, re-executing THIS binary with the same server args plus the
    // receiver-role env, inheriting the single client listener fd (no-RST). `current_exe` + the
    // original argv reproduce the serve invocation; `spawn_receiver_sibling` adds the handoff env.
    let program = std::env::current_exe().map_err(|e| HandoffError::Io(e.to_string()))?;
    let args_owned: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args_owned.iter().map(String::as_str).collect();
    // The single-acceptor default has exactly one client listener fd; pass it for the inherited-listener
    // handoff. (Shard-owner mode's N fds are a documented follow-up; `first()` covers the default.)
    let listen_fd = set.client_listener_fds().first().copied();
    let _child = spawn_receiver_sibling(&program, &args, &plan.socket, listen_fd)
        .map_err(|e| HandoffError::Io(e.to_string()))?;

    // Step 3: the Send coord (shards clone it; the host owns the barrier end), then accept + deliver.
    let (coord, host) = new_cutover(n);
    for (i, listener) in listeners.iter().enumerate() {
        let (stream, _addr) = tokio::time::timeout(plan.timeout, listener.accept())
            .await
            .map_err(|_| HandoffError::Timeout { phase: "accept" })?
            .map_err(|e| HandoffError::Io(e.to_string()))?;
        // Convert to a reactor-free std stream so it can cross to the shard thread; the shard re-adopts
        // it onto its own runtime (design risk 4: no stream is polled on a foreign runtime).
        let std_stream = stream
            .into_std()
            .map_err(|e| HandoffError::Io(e.to_string()))?;
        let start = CutoverStart {
            coord: std::sync::Arc::clone(&coord),
            shard: u32::try_from(i).unwrap_or(u32::MAX),
            chunk_max: plan.chunk_max,
            stream: std_stream,
        };
        if control[i].send(start).await.is_err() {
            // The shard's control receiver is gone (it already stopped): abort the whole cutover.
            return Err(HandoffError::Aborted);
        }
    }
    // Only the per-shard tasks hold coord clones now; drop the host's so a dead shard closes the
    // report channel and the barrier fail-closes to Abort rather than hanging.
    drop(coord);

    // Step 4: drive the cross-shard barrier to a commit-or-abort decision.
    drive_sender_cutover_host(host).await
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
    let mut cfg = load_config(cli)?;
    // Apply the K8s/k3s OOMKill guard so `check` reports the EFFECTIVE maxmemory the server will
    // boot with (an auto-derived cap under a cgroup memory limit), not the pre-derivation `0`.
    cfg.apply_cgroup_memory_guard();
    println!("ironcache check: configuration OK");
    println!("  bind        = {}:{}", cfg.bind, cfg.port);
    println!("  shards      = {}", cfg.shards);
    // The per-shard runtime backend (PROD-10 / #28), reported as the EFFECTIVE runtime this binary
    // + kernel will actually use -- not just the config request. io_uring is honored only on a Linux
    // io_uring-feature build with TLS off AND a kernel that provides io_uring (PROBED here, the same
    // gate the boot selection uses); every other case serves on tokio. Reporting the request alone
    // misled operators into believing a non-io_uring binary (or an incapable kernel) was on io_uring.
    let runtime_desc: String = match cfg.runtime {
        ironcache_config::RuntimeBackend::Tokio => "tokio".to_owned(),
        ironcache_config::RuntimeBackend::IoUring if cfg.tls == ironcache_config::TlsMode::On => {
            "io_uring requested -> tokio (TLS is on; the io_uring datapath is plaintext-only in v1)"
                .to_owned()
        }
        ironcache_config::RuntimeBackend::IoUring => {
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            {
                match ironcache_runtime::uring_probe::probe_uring_caps() {
                    Ok(_) => {
                        "io_uring (Linux, io_uring feature, TLS off, kernel-capable)".to_owned()
                    }
                    Err(e) => format!(
                        "io_uring requested -> tokio (this kernel cannot provide io_uring: {e})"
                    ),
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
            {
                "io_uring requested -> tokio (this binary is not a Linux build with the io_uring \
                 feature)"
                    .to_owned()
            }
        }
        ironcache_config::RuntimeBackend::IoUringRaw
            if cfg.tls == ironcache_config::TlsMode::On =>
        {
            "io_uring_raw requested -> tokio (TLS is on; the io_uring datapath is plaintext-only \
             in v1)"
                .to_owned()
        }
        ironcache_config::RuntimeBackend::IoUringRaw => {
            #[cfg(all(target_os = "linux", feature = "io_uring_raw"))]
            {
                match ironcache_runtime::uring_probe::probe_uring_caps() {
                    Ok(_) => "io_uring_raw (Linux, io_uring_raw feature, TLS off, kernel-capable)"
                        .to_owned(),
                    Err(e) => format!(
                        "io_uring_raw requested -> tokio (this kernel cannot provide io_uring: {e})"
                    ),
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "io_uring_raw")))]
            {
                "io_uring_raw requested -> tokio (this binary is not a Linux build with the \
                 io_uring_raw feature)"
                    .to_owned()
            }
        }
    };
    println!("  runtime     = {runtime_desc}");
    println!("  databases   = {}", cfg.databases);
    // The dedicated persist core (#589): report the effective knob (empty = off / no pin).
    println!(
        "  persist-cpu = {}",
        if cfg.persist_cpu.is_empty() {
            "off (no pin)"
        } else {
            cfg.persist_cpu.as_str()
        }
    );
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
    let mut cfg = load_config(cli)?;
    // Report the EFFECTIVE maxmemory (an auto-derived cgroup cap when unset), matching what the
    // server boots with -- consistent with `check` and INFO.
    cfg.apply_cgroup_memory_guard();
    println!("# effective ironcache configuration");
    println!("bind = \"{}\"", cfg.bind);
    println!("port = {}", cfg.port);
    println!("shards = {}", cfg.shards);
    println!(
        "runtime = \"{}\"",
        match cfg.runtime {
            ironcache_config::RuntimeBackend::Tokio => "tokio",
            ironcache_config::RuntimeBackend::IoUring => "io_uring",
            ironcache_config::RuntimeBackend::IoUringRaw => "io_uring_raw",
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
fn cmd_upgrade(cli: &Cli, args: &cli::UpgradeArgs) -> anyhow::Result<()> {
    use ironcache::upgrade;

    // CLUSTER MODE (#392): a completely separate orchestrator path. Branch FIRST so the single-node
    // flow below is byte-unchanged when `--cluster` is absent.
    if args.cluster {
        return cmd_upgrade_cluster(args);
    }

    // #391 PR-6 STREAMED LIVE-CUTOVER SELECTION: when a `handoff_socket` is configured, the upgrade
    // takes the STREAMED live-cutover path (a sibling receiver + a live keyspace stream over a unix
    // socket to a committed serve-flip, with a bounded sub-second write pause and no acked-write loss)
    // instead of the default #390 tmpfs SAVE -> swap -> restart. The gate is fail-SAFE toward the
    // default: a config that does not load, or one with NO `handoff_socket`, yields `None` and the
    // byte-unchanged default flow below runs -- the streamed path is entered ONLY when it is
    // explicitly configured, so a default deployment's upgrade is untouched.
    if let Some(plan) = load_config(cli)
        .ok()
        .and_then(|cfg| upgrade::drive::HandoffPlan::from_config(&cfg))
    {
        return cmd_upgrade_streamed(&plan, args);
    }

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

/// `ironcache upgrade` STREAMED live-cutover path (#391 PR-6), selected when `handoff_socket` is
/// configured. Unlike the default #390 path -- which SAVEs to tmpfs, swaps the binary, and RESTARTs
/// the unit (a short process restart) -- the streamed live cutover keeps the OLD process serving while
/// it streams its live keyspace to a freshly-spawned sibling, then flips write authority to the NEW at
/// a single committed linearization point with no acknowledged-write loss and no orphaned-backlog RST.
///
/// The live cutover is DRIVEN BY THE RUNNING SERVER (it owns the in-memory per-shard stores + rings):
/// the OLD server spawns the sibling receiver via [`upgrade::orchestrator::spawn_receiver_sibling`],
/// binds the handoff socket, and drives [`upgrade::orchestrator::run_sender_cutover`] to commit; the
/// sibling adopts the OLD's client listener fd (no RST) and runs
/// [`upgrade::orchestrator::run_receiver_cutover`]. This CLI entry validates the streamed configuration
/// and reports the selected path; it deliberately does NOT run the default destructive swap+restart
/// (which would kill the live process the cutover is meant to hand off from). The end-to-end mechanism
/// is proven by the real two-process acceptance test (`tests/upgrade_streamed_cutover.rs`).
///
/// [`upgrade::orchestrator::spawn_receiver_sibling`]: ironcache::upgrade::orchestrator::spawn_receiver_sibling
/// [`upgrade::orchestrator::run_sender_cutover`]: ironcache::upgrade::orchestrator::run_sender_cutover
/// [`upgrade::orchestrator::run_receiver_cutover`]: ironcache::upgrade::orchestrator::run_receiver_cutover
// Returns `Result` to match the `cmd_upgrade` dispatch (and the future live-drive error paths it will
// carry), even though the current selection/report step cannot itself fail.
#[allow(clippy::unnecessary_wraps)]
fn cmd_upgrade_streamed(
    plan: &ironcache::upgrade::drive::HandoffPlan,
    _args: &cli::UpgradeArgs,
) -> anyhow::Result<()> {
    tracing::info!(
        socket = %plan.socket.display(),
        "ironcache upgrade: STREAMED live-cutover path selected (handoff_socket configured). The \
         live old->new cutover is driven by the running server via the upgrade orchestrator (sibling \
         spawn + streamed handoff + committed serve-flip); the default tmpfs swap+restart is NOT run."
    );
    println!(
        "streamed live-cutover selected: handoff socket {}. The running ironcache server drives the \
         sibling spawn + live keyspace stream to a committed serve-flip (no acked-write loss, no \
         connection RST via the inherited listener). The default tmpfs swap+restart path is not used.",
        plan.socket.display()
    );
    Ok(())
}

/// `ironcache upgrade --cluster` (#392): the LIVE clustered rolling-upgrade orchestrator. Loads the
/// static actuation-map inventory, assembles a [`LiveCluster`] over the prod seams (authenticated RESP
/// observe + `CLUSTER FAILOVER`, the SSH per-node binary swap, the loopback write-freeze pauser), and
/// either PREVIEWS the plan (`--dry-run`, a single observe + print, NO action) or drives the roll to
/// completion (upgrade the replicas first, promote an upgraded in-sync replica behind the
/// failover-freeze fence, upgrade the old primary last). Fails loud + nonzero on a stall or an action
/// error, naming the blocking step.
fn cmd_upgrade_cluster(args: &cli::UpgradeArgs) -> anyhow::Result<()> {
    use ironcache::cluster_upgrade_driver::{
        CommandUpgrader, DriverConfig, FreezeCfg, LiveCluster, NodeUpgrader, PollCfg,
        RespClusterClient, SshUpgrader, ThreadSleeper, run_cluster_upgrade,
    };
    use ironcache::cluster_upgrade_inventory::{derive_plan, load_inventory};
    use ironcache::upgrade::pause::LoopbackPauser;
    use ironcache_repl::UpgradeReport;
    use std::time::Duration;

    // REQUIRED cluster inputs (clear error if missing): the inventory path + the explicit target.
    let (inventory_path, target) = args
        .require_cluster_inputs()
        .map_err(|msg| anyhow::anyhow!(msg))?;

    // Load + validate the static actuation map (fail closed on a malformed / invalid inventory).
    let inventory = load_inventory(inventory_path)
        .with_context(|| format!("loading the cluster inventory {}", inventory_path.display()))?;
    let node_count = inventory.len();

    // Assemble the driver config from the flags. The freeze drains the candidate to lag 0, so the
    // drain budget is `--drain-timeout` seconds at the driver's 100ms poll cadence.
    let drain_poll_delay = Duration::from_millis(100);
    let max_drain_polls = u32::try_from(args.drain_timeout.saturating_mul(10))
        .unwrap_or(u32::MAX)
        .max(1);
    let config = DriverConfig {
        inventory,
        target_version: target.to_owned(),
        max_lag: args.max_lag,
        poll: PollCfg::default(),
        freeze: FreezeCfg {
            pause_window_ms: args.pause_ms,
            max_drain_polls,
            drain_poll_delay,
        },
    };

    // The prod seams: authenticated RESP client (bounded per-exchange), the per-node binary-swap
    // actuator (default SSH; `--actuator-command` swaps in a local-command actuator for orchestrator /
    // docker-smoke deployments), the shipped loopback write-freeze pauser (pointed at the old primary
    // during a promotion), a real sleeper.
    let client = RespClusterClient::new(Duration::from_secs(args.per_node_timeout))
        .context("building the RESP cluster client")?;
    let upgrader: Box<dyn NodeUpgrader> = match args.actuator_command.as_deref() {
        Some(template) => Box::new(CommandUpgrader::from_template(template).ok_or_else(|| {
            anyhow::anyhow!("--actuator-command is empty (needs at least a program to run)")
        })?),
        None => Box::new(SshUpgrader::new()),
    };
    let mut live = LiveCluster::new(
        client,
        upgrader,
        Box::new(LoopbackPauser),
        Box::new(ThreadSleeper),
        config,
    );

    // DRY-RUN: OBSERVE once, print the derived plan, take NO action.
    if args.dry_run {
        live.refresh()
            .context("observing the cluster for the dry-run plan")?;
        let plan = derive_plan(live.view());
        print!("{plan}");
        println!(
            "dry-run only: NO action taken. Re-run without --dry-run to execute the roll of {node_count} node(s)."
        );
        return Ok(());
    }

    println!(
        "cluster rolling upgrade: {node_count} node(s) -> target {target} (replicas first, primary upgraded last, RPO=0 failover-freeze)"
    );
    match run_cluster_upgrade(&mut live, args.max_ticks) {
        Ok(UpgradeReport::Completed) => {
            tracing::info!(target = %target, nodes = node_count, "ironcache upgrade --cluster: SUCCESS");
            println!(
                "cluster upgrade succeeded: every node is on {target} (primary upgraded last)"
            );
            Ok(())
        }
        Ok(UpgradeReport::StalledAfterBudget(step)) => {
            let why = describe_stall(step);
            tracing::error!(target = %target, ?step, "ironcache upgrade --cluster: STALLED");
            Err(anyhow::anyhow!(
                "cluster upgrade did not finish within --max-ticks {}: stalled at {why}",
                args.max_ticks
            ))
        }
        Err(e) => {
            tracing::error!(error = %e, "ironcache upgrade --cluster: FAILED");
            Err(anyhow::Error::new(e).context("ironcache upgrade --cluster failed"))
        }
    }
}

/// Describe the step a stalled cluster upgrade got stuck on, for the operator-facing error (#392).
fn describe_stall(step: ironcache_repl::UpgradeStep) -> String {
    use ironcache_repl::{BlockReason, UpgradeStep};
    match step {
        UpgradeStep::UpgradeReplica => "upgrading a replica (a node upgrade did not complete)".to_owned(),
        UpgradeStep::AwaitInSync => {
            "awaiting a just-upgraded replica to catch back up (it never re-synced)".to_owned()
        }
        UpgradeStep::Promote => "promoting an upgraded in-sync replica".to_owned(),
        UpgradeStep::UpgradeOldPrimary => "upgrading the demoted old primary (last node)".to_owned(),
        UpgradeStep::Blocked(BlockReason::NoQuorum) => {
            "BLOCKED: the config-plane raft has no quorum, so the promotion fence cannot commit"
                .to_owned()
        }
        UpgradeStep::Blocked(BlockReason::NoInSyncCandidate) => {
            "BLOCKED: no upgraded replica is in sync enough to promote without losing acknowledged writes"
                .to_owned()
        }
        UpgradeStep::Done => "completed".to_owned(),
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

    // The exported `malloc_conf` string (#512, ADR-0006). Asserts the compile-time
    // string always carries the ADR-0006 background-purge + decay defaults, and that
    // the `thp:` huge-page token appears IF AND ONLY IF this is a Linux build with the
    // default-off `hugepages` feature on. This locks in the Linux gating (no `thp:`
    // token on non-Linux, so no jemalloc "Invalid conf pair" warning there) and the
    // opt-in default (no `thp:` token without the feature). Gated to non-MSVC, where
    // the `MALLOC_CONF_CSTR` const (and jemalloc itself) exist.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn malloc_conf_carries_thp_only_on_linux_with_the_hugepages_feature() {
        let conf = MALLOC_CONF_CSTR
            .to_str()
            .expect("malloc_conf is valid UTF-8");
        // The ADR-0006 defaults are always present.
        assert!(
            conf.contains("background_thread:true"),
            "malloc_conf keeps the background purge thread: {conf}"
        );
        assert!(
            conf.contains("dirty_decay_ms:5000"),
            "malloc_conf keeps the sub-10 s dirty decay: {conf}"
        );
        // THP is present exactly when Linux AND the hugepages feature are both on.
        let want_thp = cfg!(target_os = "linux") && cfg!(feature = "hugepages");
        assert_eq!(
            conf.contains("thp:always"),
            want_thp,
            "thp:always present iff Linux + hugepages feature (was {conf:?})"
        );
        // No `thp:` token at all off that path, so non-Linux jemalloc never warns.
        if !want_thp {
            assert!(
                !conf.contains("thp:"),
                "no thp token unless Linux + hugepages: {conf}"
            );
        }
    }
}
