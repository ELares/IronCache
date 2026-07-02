// SPDX-License-Identifier: MIT OR Apache-2.0
//! `ironcache upgrade`: the operator-run, verified, data-safe, health-gated, auto-rolling-back
//! binary self-updater (#387, "mechanism first, sign next" / docs/design/UPGRADE.md).
//!
//! This is the Phase 0-1 MVP MECHANISM. It swaps the on-disk binary to a new version and restarts
//! the systemd-managed server onto it, DATA-SAFELY (it triggers a synchronous fsync'd `SAVE` first
//! so the in-memory working set survives the restart) and SAFELY (it verifies the new artifact's
//! sha256 against a `SHA256SUMS`, sanity-checks the new binary runs and reports its version,
//! swaps atomically while keeping exactly one rollback slot, health-gates the restarted server,
//! and auto-rolls-back on any miss).
//!
//! ## Seams left for the EXPLICIT follow-ups
//!
//! - The CRYPTOGRAPHIC SIGNATURE ANCHOR (#386): the [`Verifier`] trait is where minisign/sigstore
//!   slots in. v1 ships only [`verify::Sha256Verifier`] (integrity, not authenticity); a
//!   `MinisignVerifier` implementing the SAME trait lands in #386 with NO change to the
//!   orchestrator.
//! - HTTPS / GitHub-latest AUTO-FETCH (#387 follow-up): the [`source::BinarySource`] trait is the
//!   fetch seam. v1 ships only [`source::LocalFile`] (the operator supplies the new binary on
//!   disk); `HttpsUrl` / `GithubLatest` land later behind the same trait.
//! - The LOSSLESS WRITE-FREEZE (#388, SHIPPED): before the final `SAVE`, the orchestrator issues a
//!   node-wide `CLIENT PAUSE <ms> WRITE` (the [`pause`] seam) and lets in-flight writes drain, so the
//!   snapshot captures a state after which NO acknowledged write is lost across the restart. It is
//!   default-ON (losslessness is the point) with a `--no-freeze` escape hatch that restores v1's
//!   SAVE-first-only behavior. The streamed/handoff (zero-downtime) lossless variant is Phase 2.
//!
//! ## Not a server surface
//!
//! Upgrade is a PRIVILEGED, OPERATOR-RUN subcommand of the short-lived CLI process. It is NEVER
//! exposed over RESP and never mutates a running server implicitly (it connects to the running
//! server only to trigger the `SAVE` and to health-probe the restarted one). `systemctl` is invoked
//! via [`std::process::Command`] from THIS short-lived process (not the server), which is NOT a
//! `fork` syscall and respects the no-fork invariant (invariant 4).

pub mod fetch;
pub mod health;
pub mod minisign;
pub mod pause;
pub mod proc;
pub mod save;
pub mod service;
pub mod source;
pub mod swap;
pub mod verify;

use std::path::{Path, PathBuf};
use std::time::Duration;

use health::{HealthProbe, HealthTarget, LoopbackProbe};
use pause::{LoopbackPauser, PauseError, PauseTarget, Pauser};
use save::{LoopbackSaver, SaveError, SaveTarget, Saver};
use service::{ServiceManager, SystemdManager};
use source::{BinarySource, LocalFile};
use swap::SwapError;
use verify::{MinisignVerifier, Sha256Verifier, Verifier, VerifyError};

/// The default live binary path the swap targets when neither `--target` nor a unit `ExecStart`
/// override is given (matches `packaging/ironcache.service`'s `ExecStart`).
pub const DEFAULT_TARGET: &str = "/usr/local/bin/ironcache";

/// The default systemd unit name (`systemctl restart ironcache`).
pub const DEFAULT_UNIT: &str = "ironcache";

/// The default health-gate budget: how long, after the restart, the orchestrator polls for the new
/// server to come back ready + on the expected version before declaring the upgrade failed (and, by
/// default, auto-rolling-back). UPGRADE.md's resolved default.
pub const DEFAULT_HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

/// The default ops endpoint the health gate probes `/readyz` on, and the RESP port it `PING`s
/// (host shared). The metrics endpoint is `127.0.0.1:9121` by convention; the RESP port defaults to
/// the cache's `6379`. Both overridable via [`UpgradeArgs`].
pub const DEFAULT_READYZ_ADDR: &str = "127.0.0.1:9121";

/// The resolved, validated arguments for one `ironcache upgrade` run. Built from the clap surface
/// (see `cli::UpgradeArgs`) by [`UpgradeArgs::from_cli`]; kept as a plain owned struct so the
/// orchestrator is testable without clap.
// A CLI argument bag: the four independent boolean operator flags (`no_rollback`, `yes`,
// `allow_same`, `no_freeze`) are orthogonal toggles, not a state machine, so the bool fields mirror
// the clap surface 1:1; a two-variant-enum refactor would not improve clarity here.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct UpgradeArgs {
    /// The new ironcache binary to install (REQUIRED for v1: the local source). Its sha256 must
    /// match its entry in `sha256sums`, and it must run + report a version.
    pub binary: PathBuf,
    /// The release `SHA256SUMS` to verify `binary` against (its sha256 must equal the entry whose
    /// filename matches `binary`'s file name).
    pub sha256sums: PathBuf,
    /// The live binary path to swap onto (default [`DEFAULT_TARGET`]). The `.new`/`.old` slots live
    /// alongside it on the SAME filesystem.
    pub target: PathBuf,
    /// The systemd unit to restart (default [`DEFAULT_UNIT`]).
    pub unit: String,
    /// The ops endpoint the health gate probes `/readyz` on (default [`DEFAULT_READYZ_ADDR`]).
    pub readyz_addr: String,
    /// The RESP `host:port` for the SAVE-first connection and the `PING` health probe.
    pub resp_addr: String,
    /// An optional `requirepass` for the loopback SAVE / PING connections, read from a FILE so it
    /// never lands in argv/logs.
    pub auth: Option<String>,
    /// The health-gate budget (default [`DEFAULT_HEALTH_TIMEOUT`]).
    pub health_timeout: Duration,
    /// Skip auto-rollback on a failed health gate (leave the new binary in place + report failure).
    pub no_rollback: bool,
    /// Skip the interactive confirm prompt (operator-asserted go-ahead). Also permits proceeding
    /// with NO persistence configured (accepting the in-memory data loss) and with a SAME-version
    /// target.
    pub yes: bool,
    /// Permit upgrading to the SAME version already installed (a re-install / repair) without
    /// `--yes`. Off by default (a same-version upgrade is usually an operator mistake).
    pub allow_same: bool,
    /// Opt OUT of the lossless write-freeze (#388): when set, behave exactly as v1 (SAVE-first only,
    /// accepting the tiny window where the old process can ack a write not in the snapshot). Off by
    /// default -- the freeze is on, because losslessness is the point. An operator who accepts the
    /// window (a read-mostly or rebuildable cache) can set this. The freeze only matters when
    /// persistence is ON; with no persistence the restart loses data regardless, so the freeze is
    /// skipped there irrespective of this flag.
    pub no_freeze: bool,
}

/// A typed upgrade failure (ERRORS.md: no stringly-typed errors). Each variant pins WHICH stage
/// failed so the operator log + the exit code are unambiguous; none of these are panics (every IO /
/// parse error is mapped here).
#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    /// Resolving / fetching the new binary failed (the [`BinarySource`] seam).
    #[error("resolving the new binary: {0}")]
    Source(#[from] source::SourceError),
    /// Verification failed: a sha256 mismatch, a missing/malformed `SHA256SUMS` entry, or the new
    /// binary failed to run / report a version (the [`Verifier`] seam).
    #[error("verifying the new binary: {0}")]
    Verify(#[from] VerifyError),
    /// The new binary reports the SAME version already installed and neither `--allow-same` nor
    /// `--yes` was given (a same-version upgrade is refused as a likely mistake).
    #[error(
        "the new binary reports version {version}, which is already installed; \
         pass --allow-same (or --yes) to re-install the same version"
    )]
    SameVersion {
        /// The version both the on-disk current binary and the new binary report.
        version: String,
    },
    /// Persistence is NOT configured (no reachable server / no `data_dir`), so the restart would
    /// lose the in-memory working set, and `--yes` was not given to accept that.
    #[error(
        "SAVE-first could not confirm a persisted snapshot ({reason}); the restart would lose \
         in-memory data. Configure a data_dir (persistence), or pass --yes to accept the loss \
         (the lossless write-freeze is the #388 follow-up)"
    )]
    NoPersistence {
        /// Why SAVE-first could not confirm a current snapshot.
        reason: String,
    },
    /// The SAVE-first step failed for a reason OTHER than "no persistence" (e.g. a connect/auth/IO
    /// error, or the server reported the save did not advance). This is FATAL (we do not swap over
    /// an unconfirmed save) regardless of `--yes`, because it means we cannot reason about data
    /// safety at all (distinct from the honest "no persistence configured" case). When the
    /// write-freeze was already in place, the orchestrator UNPAUSEs the still-living old process
    /// before returning this, so an aborted upgrade never leaves production write-frozen.
    #[error("SAVE-first failed: {source}{unfreeze}")]
    Save {
        /// The underlying SAVE-first error.
        #[source]
        source: SaveError,
        /// A human-readable suffix describing the abort-time UNPAUSE outcome (empty when the freeze
        /// was not in place, e.g. under `--no-freeze` or no persistence).
        unfreeze: String,
    },
    /// The lossless write-freeze (#388) could not be established (connect/auth/protocol/timeout on
    /// the loopback `CLIENT PAUSE`). FATAL: we promised no acknowledged write would be lost, and we
    /// could not freeze writes, so we refuse to swap (no on-disk change was made). The old process is
    /// untouched (it was never frozen). Use `--no-freeze` to accept the tiny window and proceed
    /// SAVE-first-only.
    #[error("the lossless write-freeze failed: {0}")]
    Freeze(#[from] PauseError),
    /// The atomic swap (or its rollback) hit a filesystem error: a cross-device rename (EXDEV), a
    /// permission error, or a missing slot (the [`swap`] seam). The swap runs AFTER the write-freeze,
    /// while the old process is still alive, so on a failure the orchestrator UNPAUSEs it (best
    /// effort) before returning -- a failed swap never leaves production write-frozen.
    #[error("swapping the binary: {source}{unfreeze}")]
    Swap {
        /// The underlying filesystem error.
        #[source]
        source: SwapError,
        /// A human-readable suffix describing the abort-time UNPAUSE outcome (empty when the freeze
        /// was not in place, e.g. under `--no-freeze`).
        unfreeze: String,
    },
    /// Restarting the unit via the [`ServiceManager`] failed. The restart is the step that would have
    /// killed the old (frozen) process; on a failure the old process may still be alive AND frozen, so
    /// the orchestrator UNPAUSEs it (best effort -- harmless if the process did die) before returning.
    #[error("restarting unit {unit}: {source}{unfreeze}")]
    Restart {
        /// The unit that failed to restart.
        unit: String,
        /// The underlying service-manager error.
        source: service::ServiceError,
        /// A human-readable suffix describing the abort-time UNPAUSE outcome (empty when the freeze
        /// was not in place, e.g. under `--no-freeze`).
        unfreeze: String,
    },
    /// The health gate failed: the restarted server did not come back ready + on the expected
    /// version within the budget. Carries the probe's reason and whether auto-rollback ran + its
    /// outcome, so the final summary is self-describing.
    #[error("health gate failed after the swap: {reason}{rollback}")]
    HealthGate {
        /// Why the new server failed the health gate.
        reason: String,
        /// A human-readable suffix describing the rollback outcome (or that it was skipped).
        rollback: String,
    },
    /// The `/readyz` endpoint is not reachable BEFORE the swap, so the health gate could never run.
    /// Failed early (no on-disk change) rather than swapping then auto-rolling-back a healthy binary.
    #[error(
        "the unit does not expose /readyz at {addr}; the health gate cannot run -- add \
         `--metrics-addr {addr}` to the unit's ExecStart (no on-disk change was made): {reason}"
    )]
    ReadyzPreflight {
        /// The readyz address that was unreachable.
        addr: String,
        /// The underlying probe reason.
        reason: String,
    },
    /// The operator declined the confirm prompt.
    #[error("upgrade aborted by the operator at the confirm prompt")]
    Aborted,
    /// Reading the auth password file failed.
    #[error("reading --auth-file {path}: {source}")]
    AuthFile {
        /// The auth file path.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },
}

/// The injected collaborators the orchestrator drives, so the flow is unit-testable with mocks. The
/// production entry point [`run`] wires the concrete ([`Sha256Verifier`], [`LocalFile`],
/// [`SystemdManager`], [`LoopbackProbe`]) implementations; tests inject fakes.
pub struct UpgradeDeps<S, V, M, P, Sv, Pz>
where
    S: BinarySource,
    V: Verifier,
    M: ServiceManager,
    P: HealthProbe,
    Sv: Saver,
    Pz: Pauser,
{
    /// Where the new binary bytes come from (LocalFile in v1).
    pub source: S,
    /// What proves the new binary is the right one (sha256 in v1; minisign later, #386).
    pub verifier: V,
    /// How the server process is restarted (systemd in v1).
    pub service: M,
    /// How the restarted server's readiness + version is probed.
    pub probe: P,
    /// How the in-memory working set is made durable before the swap (loopback SAVE in v1).
    pub saver: Sv,
    /// How writes are FROZEN before the final SAVE so no acknowledged write is lost (#388): a
    /// loopback `CLIENT PAUSE <ms> WRITE` in v1.
    pub pauser: Pz,
}

/// The summary of a SUCCESSFUL upgrade, for the final structured log + the caller.
#[derive(Debug, Clone)]
pub struct UpgradeOutcome {
    /// The version now installed + confirmed running.
    pub installed_version: String,
    /// The version that was installed before (the retained `.old` slot, for one-shot rollback).
    pub previous_version: Option<String>,
    /// Whether a SAVE-first snapshot was confirmed current before the swap (false only under
    /// `--yes` with no persistence).
    pub save_confirmed: bool,
}

/// PRODUCTION entry point: run the upgrade with the concrete collaborators. Called by
/// `cmd_upgrade`. Confirms with the operator (unless `--yes`), then drives [`run_with`].
///
/// # Errors
///
/// Returns an [`UpgradeError`] on any stage failure; the caller maps it to a nonzero exit.
pub fn run(args: &UpgradeArgs) -> Result<UpgradeOutcome, UpgradeError> {
    // Select the verifier by the pinned minisign anchor (#386): if a public key is committed
    // ([`verify::PINNED_UPGRADE_PUBLIC_KEY`]), upgrades REQUIRE a valid minisign signature over
    // `SHA256SUMS` (authenticity); otherwise the integrity-only `Sha256Verifier` (the current default,
    // until the key is committed + the release workflow signs). Both go through the SAME orchestrator.
    match verify::PINNED_UPGRADE_PUBLIC_KEY {
        Some(pubkey) => run_with_verifier(args, MinisignVerifier::new(pubkey)?),
        None => run_with_verifier(args, Sha256Verifier),
    }
}

/// Build the production dependency set with the chosen `verifier` and drive the orchestrator. The
/// verifier is the only production dependency that varies (by the pinned minisign anchor, #386); the
/// rest are the fixed loopback/systemd implementations.
fn run_with_verifier<V: Verifier>(
    args: &UpgradeArgs,
    verifier: V,
) -> Result<UpgradeOutcome, UpgradeError> {
    let deps = UpgradeDeps {
        source: LocalFile::new(args.binary.clone()),
        verifier,
        service: SystemdManager,
        probe: LoopbackProbe,
        saver: LoopbackSaver,
        pauser: LoopbackPauser,
    };
    run_with(args, deps, &mut StderrConfirm)
}

/// A yes/no confirm seam so the prompt is mockable in tests (the real one reads stdin). `confirm`
/// returns `true` to proceed.
pub trait Confirm {
    /// Ask the operator to proceed with the described upgrade; return `true` to go ahead.
    fn confirm(&mut self, summary: &str) -> bool;
}

/// The production confirm: print the summary to stderr and read a `y`/`yes` from stdin. A read
/// error (no TTY) is treated as a decline (fail-safe), so a non-interactive invocation must pass
/// `--yes`.
pub struct StderrConfirm;

impl Confirm for StderrConfirm {
    fn confirm(&mut self, summary: &str) -> bool {
        use std::io::Write as _;
        let mut err = std::io::stderr();
        let _ = writeln!(err, "{summary}");
        let _ = write!(err, "Proceed with the upgrade? [y/N]: ");
        let _ = err.flush();
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(_) => {
                let ans = line.trim().to_ascii_lowercase();
                ans == "y" || ans == "yes"
            }
            Err(_) => false,
        }
    }
}

/// The ORCHESTRATOR, generic over the collaborators so it is unit-testable with mocks. Drives the
/// full UPGRADE.md flow:
///
/// 1. RESOLVE the new binary bytes ([`BinarySource`]).
/// 2. VERIFY its sha256 against `SHA256SUMS` ([`Verifier`]), and sanity-check it RUNS + reports a
///    version (the EXPECTED target version). Refuse on a mismatch, and refuse a same-version target
///    unless `--allow-same`/`--yes`.
/// 3. CONFIRM with the operator (unless `--yes`).
/// 4. WRITE-FREEZE (#388, unless `--no-freeze` or no persistence): issue a node-wide
///    `CLIENT PAUSE <ms> WRITE` and let in-flight writes drain, so the upcoming `SAVE` captures a
///    state after which no acknowledged write is lost. On ANY abort after this point and before the
///    restart, the old process is UNPAUSEd so production is never left frozen.
/// 5. SAVE-FIRST: trigger a synchronous fsync'd `SAVE` and confirm `LASTSAVE` advanced, so the
///    in-memory working set survives the restart. With NO persistence, require `--yes`.
/// 6. SWAP atomically (`target -> target.old`, `target.new -> target`), keeping one rollback slot.
/// 7. RESTART the unit ([`ServiceManager`]): the old (frozen) process dies here, so the freeze needs
///    no explicit unpause; the new process boots UNPAUSED from the complete snapshot.
/// 8. HEALTH-GATE within the budget ([`HealthProbe`]: `/readyz` 200 + `PING -> PONG` + the on-disk
///    `target --version` equals the expected target).
/// 9. AUTO-ROLLBACK on any miss (unless `--no-rollback`): restore `.old`, restart, re-probe.
///
/// # Errors
///
/// Returns the first stage's [`UpgradeError`]; on a health-gate miss the error records the rollback
/// outcome.
pub fn run_with<S, V, M, P, Sv, Pz>(
    args: &UpgradeArgs,
    deps: UpgradeDeps<S, V, M, P, Sv, Pz>,
    confirm: &mut dyn Confirm,
) -> Result<UpgradeOutcome, UpgradeError>
where
    S: BinarySource,
    V: Verifier,
    M: ServiceManager,
    P: HealthProbe,
    Sv: Saver,
    Pz: Pauser,
{
    let UpgradeDeps {
        source,
        verifier,
        service,
        probe,
        saver,
        pauser,
    } = deps;

    // 1. RESOLVE the new binary (its on-disk path + its file name, for the SHA256SUMS entry lookup).
    let resolved = source.resolve()?;
    tracing::info!(
        binary = %resolved.path.display(),
        name = %resolved.name,
        "upgrade: resolved the new binary"
    );

    // 2. VERIFY + version-sanity, and 3. CONFIRM. Returns the expected target version + the current
    // installed version (best-effort).
    let (target_version, current_version) =
        verify_and_confirm(&verifier, &resolved, args, confirm)?;

    // 4 + 4b. WRITE-FREEZE -> SAVE-FIRST -> READYZ PRE-FLIGHT, in the lossless order. The freeze (a
    // node-wide `CLIENT PAUSE WRITE`, unless `--no-freeze`) is issued BEFORE the SAVE so the SAVE
    // captures a state after which no acknowledged write is lost; on ANY abort after the freeze and
    // before the restart, the old process is UNPAUSEd so production is never left frozen. The
    // pre-flight is inside the freeze window too: a pre-flight failure aborts before the swap, so the
    // freeze must be lifted there as well. (When the SAVE then reports NO persistence, the freeze was
    // a harmless no-op write-pause -- it cannot help a non-persisted cache -- cleared by the abort
    // path or by the dying old process under `--yes`.)
    // `freeze_state` carries whether the write-freeze is ACTIVE (it persists past this call, through
    // the swap + restart, where the dying old process clears it) so the swap-failure / restart-failure
    // paths below -- which also run with the old, still-frozen process alive -- can lift it.
    let (save_confirmed, freeze_state) = freeze_save_preflight(&pauser, &saver, &probe, args)?;

    // 4c. Capture the PRE-RESTART uptime baseline U0 (review fix #2): the gate requires the
    // post-restart uptime to be SMALL and STRICTLY BELOW this, so a no-op restart / stale process
    // (large increasing uptime) cannot pass the gate.
    let baseline_uptime = probe.baseline_uptime(&args.readyz_addr);
    tracing::info!(
        baseline_uptime = baseline_uptime.unwrap_or(0),
        have_baseline = baseline_uptime.is_some(),
        "upgrade: captured the pre-restart uptime baseline"
    );

    // 5. SWAP atomically (never-absent single-rename idiom): stage <target>.new, hard-link (or copy)
    // the current target to <target>.old, then ONE atomic rename(.new -> target). target is NEVER
    // absent; never opens the live executable for write (no ETXTBSY). A swap FAILURE returns while the
    // old (frozen) process is still alive, so UNPAUSE it first -- a failed swap never leaves prod
    // write-frozen.
    if let Err(source) = swap::swap(&resolved.path, &args.target) {
        return Err(unfreeze_on_abort(
            &pauser,
            &freeze_state,
            UpgradeError::Swap {
                source,
                unfreeze: String::new(),
            },
        ));
    }
    tracing::info!(target = %args.target.display(), "upgrade: atomic swap complete (.old retained)");

    // 6. RESTART the unit onto the new binary. This is the step that KILLS the old (frozen) process,
    // clearing the freeze on the happy path. A restart FAILURE may leave the old process alive AND
    // frozen, so UNPAUSE it (best effort -- harmless if it actually did die) before returning.
    if let Err(source) = service.restart(&args.unit) {
        return Err(unfreeze_on_abort(
            &pauser,
            &freeze_state,
            UpgradeError::Restart {
                unit: args.unit.clone(),
                source,
                unfreeze: String::new(),
            },
        ));
    }
    tracing::info!(unit = %args.unit, "upgrade: restart issued");

    // 7. HEALTH-GATE: poll until the new server is restarted + stabilized + ready + on the expected
    // version, or the budget elapses.
    let htarget = HealthTarget {
        readyz_addr: args.readyz_addr.clone(),
        resp_addr: args.resp_addr.clone(),
        binary: args.target.clone(),
        expected_version: target_version.clone(),
        auth: args.auth.clone(),
        baseline_uptime,
    };
    match probe.gate(&htarget, args.health_timeout) {
        Ok(()) => {
            tracing::info!(
                version = %target_version,
                "upgrade: health gate PASSED -- new binary promoted (.old kept for one-shot rollback)"
            );
            Ok(UpgradeOutcome {
                installed_version: target_version,
                previous_version: current_version,
                save_confirmed,
            })
        }
        Err(reason) => {
            // 8. AUTO-ROLLBACK on any miss (unless --no-rollback).
            let reason = reason.to_string();
            tracing::error!(%reason, "upgrade: health gate FAILED");
            if args.no_rollback {
                Err(UpgradeError::HealthGate {
                    reason,
                    rollback: " (--no-rollback: the new binary is left in place)".to_owned(),
                })
            } else {
                let rollback = perform_rollback(&service, &probe, args, current_version.as_deref());
                Err(UpgradeError::HealthGate { reason, rollback })
            }
        }
    }
}

/// Steps 2-3: VERIFY the resolved binary's sha256, SANITY-check it runs + reports a version (the
/// expected target version), refuse a same-version target (unless `--allow-same`/`--yes`), and
/// CONFIRM with the operator (unless `--yes`). Returns `(target_version, current_version)` where the
/// current version is `None` for a fresh install (no/unrunnable existing binary).
fn verify_and_confirm<V: Verifier>(
    verifier: &V,
    resolved: &source::ResolvedBinary,
    args: &UpgradeArgs,
    confirm: &mut dyn Confirm,
) -> Result<(String, Option<String>), UpgradeError> {
    // 2a. Integrity: the binary's sha256 must match its SHA256SUMS entry.
    verifier.verify(&resolved.path, &resolved.name, &args.sha256sums)?;
    tracing::info!(name = %resolved.name, "upgrade: sha256 verified against SHA256SUMS");

    // 2b. Sanity: the new binary must RUN and report a version (the expected target version).
    let target_version = verify::probe_binary_version(&resolved.path)?;
    // The version currently installed. DISTINGUISH (review fix #8) a legit FRESH install (no binary
    // at the target path) from a PRESENT-but-UNRUNNABLE binary: a probe failure on an existing file
    // is NOT a fresh install (it would silently skip the same-version guard), so we warn loudly and
    // still treat the current version as unknown (we cannot read it), rather than pretending it is
    // absent.
    let current_version = if args.target.exists() {
        match verify::probe_binary_version(&args.target) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    target = %args.target.display(),
                    error = %e,
                    "upgrade: the EXISTING target binary is present but did not report a version \
                     (corrupt / incompatible?); proceeding, but the same-version guard cannot apply"
                );
                None
            }
        }
    } else {
        // No binary at the target path: a genuine fresh install.
        None
    };
    tracing::info!(
        target_version = %target_version,
        current_version = current_version.as_deref().unwrap_or("(unknown)"),
        "upgrade: version sanity"
    );
    if current_version.as_deref() == Some(target_version.as_str()) && !args.allow_same && !args.yes
    {
        return Err(UpgradeError::SameVersion {
            version: target_version,
        });
    }

    // 3. Confirm with the operator (unless --yes).
    if !args.yes {
        let summary = format!(
            "ironcache upgrade:\n  target  = {}\n  unit    = {}\n  from    = {}\n  to      = {}\n  \
             rollback= {}",
            args.target.display(),
            args.unit,
            current_version.as_deref().unwrap_or("(unknown)"),
            target_version,
            if args.no_rollback {
                "DISABLED (--no-rollback)"
            } else {
                "auto (restores the prior binary on a failed health gate)"
            },
        );
        if !confirm.confirm(&summary) {
            return Err(UpgradeError::Aborted);
        }
    }
    Ok((target_version, current_version))
}

/// Steps 4 + 4b: the LOSSLESS pre-swap sequence. FREEZE writes (unless `--no-freeze`), SAVE-FIRST,
/// then PRE-FLIGHT the health endpoint -- all inside the freeze window, so an abort in ANY of them
/// (before any on-disk change) lifts the freeze. Returns `save_confirmed` (`true` when the snapshot
/// is confirmed current, `false` when persistence is off and `--yes` accepts the loss).
///
/// ## Order (why it is lossless)
///
/// The freeze (`CLIENT PAUSE <ms> WRITE` + drain) is issued FIRST, so when the SAVE runs no further
/// write can be acknowledged: the SAVE captures a state after which nothing acked exists outside it.
/// On the happy path the old process later dies at the restart and the freeze evaporates with it (no
/// explicit unpause). On ANY abort here (a failed SAVE, no persistence without `--yes`, or a failed
/// pre-flight) the old process keeps running, so we issue a best-effort `CLIENT UNPAUSE` before
/// returning the error -- production is never left write-frozen.
///
/// ## When the freeze applies
///
/// Only when persistence is ON (the freeze is irrelevant otherwise: the restart loses the in-memory
/// set regardless) and `--no-freeze` is not set. We do not know persistence is on until the SAVE
/// runs, so we FREEZE first and, if the SAVE then reports no persistence, the freeze was a harmless
/// no-op write-pause that the abort path (or the dying old process) clears.
fn freeze_save_preflight<Pz: Pauser, Sv: Saver, P: HealthProbe>(
    pauser: &Pz,
    saver: &Sv,
    probe: &P,
    args: &UpgradeArgs,
) -> Result<(bool, FreezeState), UpgradeError> {
    let ptarget = PauseTarget {
        resp_addr: args.resp_addr.clone(),
        auth: args.auth.clone(),
        window_ms: pause::derive_pause_window(args.health_timeout),
    };

    // 4a. FREEZE writes BEFORE the SAVE (unless opted out). A freeze failure is FATAL and made BEFORE
    // any on-disk change; the old process was never frozen, so there is nothing to unpause.
    let frozen = if args.no_freeze {
        tracing::warn!(
            "upgrade: --no-freeze: skipping the lossless write-freeze; proceeding SAVE-first only \
             (a write acked between the SAVE and the restart could be lost)"
        );
        false
    } else {
        pauser.freeze(&ptarget)?;
        tracing::info!(
            window_ms = ptarget.window_ms,
            "upgrade: write-freeze engaged (CLIENT PAUSE WRITE); in-flight writes drained"
        );
        true
    };
    // The freeze remains ACTIVE on Ok return: it must persist through the swap + restart (the old
    // process is killed by the restart, which clears it). The caller threads this state so the
    // swap-failure / restart-failure paths -- which also run with the old process still alive -- can
    // lift the freeze too (see `unfreeze_on_abort`).
    let state = FreezeState { frozen, ptarget };

    // 4b. SAVE-FIRST, then 4c. PRE-FLIGHT -- both inside the freeze window. On any error, lift the
    // freeze (if we set one) before surfacing the error, so an aborted upgrade never leaves prod
    // write-frozen.
    match save_then_preflight(saver, probe, args) {
        Ok(save_confirmed) => Ok((save_confirmed, state)),
        Err(err) => Err(unfreeze_on_abort(pauser, &state, err)),
    }
}

/// The write-freeze state threaded out of [`freeze_save_preflight`] so the orchestrator can lift the
/// freeze on a swap-failure / restart-failure too (both run with the old, still-frozen process alive).
/// `frozen` is the SAME gate the freeze step used (`!--no-freeze` and the freeze actually went in);
/// `ptarget` is the loopback target an abort-time `CLIENT UNPAUSE` connects to.
struct FreezeState {
    /// Whether a freeze is actually in place (false under `--no-freeze`).
    frozen: bool,
    /// The loopback pause target (resp addr / auth / window) for an abort-time unpause.
    ptarget: PauseTarget,
}

/// The SAVE-first + readyz pre-flight pair, factored so [`freeze_save_preflight`] can wrap it in the
/// unfreeze-on-abort guard. Returns `save_confirmed`; any error is one the caller must unfreeze for.
fn save_then_preflight<Sv: Saver, P: HealthProbe>(
    saver: &Sv,
    probe: &P,
    args: &UpgradeArgs,
) -> Result<bool, UpgradeError> {
    // SAVE-FIRST (data safety): make the in-memory working set durable BEFORE the restart.
    let target = SaveTarget {
        resp_addr: args.resp_addr.clone(),
        auth: args.auth.clone(),
    };
    let save_confirmed = match saver.save_first(&target) {
        Ok(save::SaveOutcome::Confirmed { last_save }) => {
            tracing::info!(
                last_save,
                "upgrade: SAVE-first confirmed (LASTSAVE advanced)"
            );
            true
        }
        Ok(save::SaveOutcome::NoPersistence { reason }) => {
            if args.yes {
                tracing::warn!(
                    %reason,
                    "upgrade: persistence is NOT configured; proceeding under --yes, the restart \
                     WILL LOSE in-memory data (the write-freeze cannot help a non-persisted cache)"
                );
                false
            } else {
                return Err(UpgradeError::NoPersistence { reason });
            }
        }
        Err(source) => {
            return Err(UpgradeError::Save {
                source,
                unfreeze: String::new(),
            });
        }
    };

    // PRE-FLIGHT the health endpoint (review fix #3): if nothing is listening on the readyz addr, the
    // health gate could NEVER pass, so we would swap-then-rollback a healthy binary. Fail EARLY with
    // an actionable error BEFORE any on-disk change.
    probe
        .preflight(&args.readyz_addr)
        .map_err(|e| UpgradeError::ReadyzPreflight {
            addr: args.readyz_addr.clone(),
            reason: e.to_string(),
        })?;

    Ok(save_confirmed)
}

/// On an abort while the write-freeze is ACTIVE and the old process is still alive (a failed SAVE /
/// pre-flight BEFORE the swap, OR a failed swap / restart AFTER it), lift the freeze on the old
/// process (best-effort), then return the original abort `err` enriched with the unfreeze outcome.
/// When `state.frozen` is `false` (no freeze was set -- `--no-freeze`) there is nothing to lift, so
/// the error passes through unchanged. A failed unfreeze is logged loudly and folded into the
/// message; the window will still self-expire (and the old process keeps serving READS throughout),
/// so it is not promoted to a different error.
///
/// A best-effort unfreeze that fails because the old process already DIED (e.g. a partial restart
/// then a failure -- connection refused) is harmless: the freeze died with it. So we attempt the
/// unfreeze on every frozen abort path, including the restart-failure path where the process may be
/// alive or dead.
fn unfreeze_on_abort<Pz: Pauser>(
    pauser: &Pz,
    state: &FreezeState,
    err: UpgradeError,
) -> UpgradeError {
    if !state.frozen {
        return err;
    }
    let ptarget = &state.ptarget;
    let note = match pauser.unfreeze(ptarget) {
        Ok(()) => {
            tracing::info!(
                "upgrade: aborted while the write-freeze was active; CLIENT UNPAUSE issued (writes \
                 resume)"
            );
            " (the write-freeze was lifted: CLIENT UNPAUSE issued)".to_owned()
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                window_ms = ptarget.window_ms,
                "upgrade: aborted while the write-freeze was active, but CLIENT UNPAUSE FAILED (the \
                 old process may have already died, which is harmless); the pause window will \
                 self-expire if the old process is still alive"
            );
            format!(
                " (WARNING: the write-freeze could not be lifted: {e}; if the old process is still \
                 alive, writes resume when the {}ms pause window expires)",
                ptarget.window_ms
            )
        }
    };
    // Enrich the abort error with the unfreeze outcome. Every variant that can be returned from a
    // frozen abort path (SAVE-first, swap, restart) carries an `{unfreeze}` slot the message renders;
    // any other variant already renders fully and passes through (the unfreeze was still logged).
    match err {
        UpgradeError::Save { source, .. } => UpgradeError::Save {
            source,
            unfreeze: note,
        },
        UpgradeError::Swap { source, .. } => UpgradeError::Swap {
            source,
            unfreeze: note,
        },
        UpgradeError::Restart { unit, source, .. } => UpgradeError::Restart {
            unit,
            source,
            unfreeze: note,
        },
        other => other,
    }
}

/// Roll back to the prior binary after a failed health gate: restore `.old` onto `target`, restart
/// the unit, and re-probe the restored binary (against the previous version, if known). Returns a
/// human-readable suffix describing the outcome for the failure summary. Never panics: every error
/// is folded into the returned string so the caller still reports the original health-gate failure.
fn perform_rollback<M: ServiceManager, P: HealthProbe>(
    service: &M,
    probe: &P,
    args: &UpgradeArgs,
    previous_version: Option<&str>,
) -> String {
    tracing::warn!(target = %args.target.display(), "upgrade: auto-rolling back to the prior binary");
    if let Err(e) = swap::rollback(&args.target) {
        return format!(
            " (ROLLBACK FAILED restoring the prior binary: {e}; manual intervention required)"
        );
    }
    // Capture a FRESH baseline before the rollback restart so the re-probe's restart-detection works
    // (the failed new server's uptime is the baseline the restored binary must reset below).
    let rollback_baseline = probe.baseline_uptime(&args.readyz_addr);
    if let Err(e) = service.restart(&args.unit) {
        return format!(
            " (rolled back the binary, but RESTART FAILED: {e}; manual intervention required)"
        );
    }
    // Re-probe the restored binary. If we know the previous version, gate on it; otherwise probe
    // readiness only (the previous binary's version is whatever it was).
    let htarget = HealthTarget {
        readyz_addr: args.readyz_addr.clone(),
        resp_addr: args.resp_addr.clone(),
        binary: args.target.clone(),
        expected_version: previous_version.unwrap_or_default().to_owned(),
        auth: args.auth.clone(),
        baseline_uptime: rollback_baseline,
    };
    let reprobe = if previous_version.is_some() {
        probe.gate(&htarget, args.health_timeout)
    } else {
        probe.gate_ready_only(&htarget, args.health_timeout)
    };
    match reprobe {
        Ok(()) => {
            tracing::info!("upgrade: rollback succeeded -- the prior binary is back and healthy");
            " (auto-rolled back to the prior binary, which is back and healthy)".to_owned()
        }
        Err(e) => format!(
            " (rolled back + restarted the prior binary, but it did not re-pass the health gate: {e}; \
             manual intervention required)"
        ),
    }
}

/// Read an optional auth password from a file (keeps the secret out of argv / logs). Trims a single
/// trailing newline (the common `echo "pw" > file` form). Returns `Ok(None)` when `path` is `None`.
///
/// # Errors
///
/// Returns [`UpgradeError::AuthFile`] when the file cannot be read.
pub fn read_auth_file(path: Option<&Path>) -> Result<Option<String>, UpgradeError> {
    match path {
        None => Ok(None),
        Some(p) => {
            let raw = std::fs::read_to_string(p).map_err(|source| UpgradeError::AuthFile {
                path: p.to_path_buf(),
                source,
            })?;
            // Trim only trailing newlines/CR so a password with intentional spaces is preserved.
            let pw = raw.trim_end_matches(['\n', '\r']).to_owned();
            Ok(if pw.is_empty() { None } else { Some(pw) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade::health::HealthTarget;
    use crate::upgrade::service::ServiceError;
    use crate::upgrade::source::ResolvedBinary;
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ---- test doubles ----

    /// A source that returns a fixed already-on-disk path (the test writes the file).
    struct FixedSource {
        path: PathBuf,
        name: String,
    }
    impl BinarySource for FixedSource {
        fn resolve(&self) -> Result<ResolvedBinary, source::SourceError> {
            Ok(ResolvedBinary {
                path: self.path.clone(),
                name: self.name.clone(),
            })
        }
    }

    /// A verifier that always passes (the orchestration tests are about the FLOW, not sha256; the
    /// sha256 path has its own unit tests in `verify`).
    struct PassVerifier;
    impl Verifier for PassVerifier {
        fn verify(
            &self,
            _binary: &Path,
            _name: &str,
            _sha256sums: &Path,
        ) -> Result<(), VerifyError> {
            Ok(())
        }
    }

    /// A mock service manager that records restart calls.
    #[derive(Clone)]
    struct MockService {
        restarts: Arc<AtomicUsize>,
        fail: bool,
    }
    impl ServiceManager for MockService {
        fn restart(&self, _unit: &str) -> Result<(), ServiceError> {
            self.restarts.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(ServiceError::CommandFailed {
                    tool: "mock".to_owned(),
                    status: "1".to_owned(),
                    stderr: "forced".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    /// A mock probe driven by a scripted queue of outcomes (one per `gate`/`gate_ready_only` call).
    /// `preflight_ok` controls the pre-flight gate; `baseline` is the captured baseline.
    struct MockProbe {
        results: RefCell<Vec<Result<(), String>>>,
        preflight_ok: bool,
        baseline: Option<u64>,
    }
    impl MockProbe {
        /// The common case: pre-flight passes, no baseline.
        fn new(results: Vec<Result<(), String>>) -> Self {
            MockProbe {
                results: RefCell::new(results),
                preflight_ok: true,
                baseline: None,
            }
        }
    }
    impl HealthProbe for MockProbe {
        fn preflight(&self, _addr: &str) -> Result<(), health::ProbeError> {
            if self.preflight_ok {
                Ok(())
            } else {
                Err(health::ProbeError::NotHealthy {
                    reason: "nothing listening on the readyz addr (mock)".to_owned(),
                })
            }
        }
        fn baseline_uptime(&self, _addr: &str) -> Option<u64> {
            self.baseline
        }
        fn gate(&self, _t: &HealthTarget, _budget: Duration) -> Result<(), health::ProbeError> {
            match self.results.borrow_mut().remove(0) {
                Ok(()) => Ok(()),
                Err(r) => Err(health::ProbeError::NotHealthy { reason: r }),
            }
        }
        fn gate_ready_only(
            &self,
            t: &HealthTarget,
            budget: Duration,
        ) -> Result<(), health::ProbeError> {
            self.gate(t, budget)
        }
    }

    /// A mock saver returning a fixed outcome, so the orchestration tests do not need a live server.
    struct MockSaver {
        outcome: save::SaveOutcome,
    }
    impl Saver for MockSaver {
        fn save_first(&self, _t: &SaveTarget) -> Result<save::SaveOutcome, SaveError> {
            Ok(self.outcome.clone())
        }
    }
    /// A saver whose `SAVE` confirms a current snapshot (the persistence-on happy path).
    fn confirming_saver() -> MockSaver {
        MockSaver {
            outcome: save::SaveOutcome::Confirmed { last_save: 1 },
        }
    }

    /// A no-op pauser for the tests that do not exercise the freeze ORDER (it records nothing and
    /// always succeeds). The default-on freeze still runs through it, so these tests prove the freeze
    /// step does not perturb the existing flow.
    struct NoopPauser;
    impl Pauser for NoopPauser {
        fn freeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            Ok(())
        }
        fn unfreeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            Ok(())
        }
    }
    fn noop_pauser() -> NoopPauser {
        NoopPauser
    }

    /// A shared, ordered event log so the orchestration-ORDER tests can assert PAUSE-before-SAVE-
    /// before-swap/restart. Each order-sensitive mock pushes its label here as it is invoked.
    type Events = Arc<std::sync::Mutex<Vec<&'static str>>>;
    fn events() -> Events {
        Arc::new(std::sync::Mutex::new(Vec::new()))
    }
    fn record(ev: &Events, label: &'static str) {
        ev.lock().unwrap().push(label);
    }

    /// An order-recording pauser: pushes `"pause"` on `freeze` and `"unpause"` on `unfreeze`.
    /// `freeze_err` forces the freeze to fail (after recording the attempt) for the freeze-fatal test.
    struct RecordingPauser {
        ev: Events,
        freeze_err: bool,
    }
    impl Pauser for RecordingPauser {
        fn freeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            record(&self.ev, "pause");
            if self.freeze_err {
                Err(PauseError::Connect {
                    addr: "127.0.0.1:6379".to_owned(),
                    detail: "forced".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        fn unfreeze(&self, _t: &PauseTarget) -> Result<(), PauseError> {
            record(&self.ev, "unpause");
            Ok(())
        }
    }

    /// An order-recording saver: pushes `"save"` then returns `outcome` (or an error when
    /// `err` is set, for the abort-after-pause test).
    struct RecordingSaver {
        ev: Events,
        outcome: save::SaveOutcome,
        err: bool,
    }
    impl Saver for RecordingSaver {
        fn save_first(&self, _t: &SaveTarget) -> Result<save::SaveOutcome, SaveError> {
            record(&self.ev, "save");
            if self.err {
                Err(SaveError::Io("forced save failure".to_owned()))
            } else {
                Ok(self.outcome.clone())
            }
        }
    }

    /// An order-recording service manager: pushes `"restart"` on each restart, then optionally FAILS
    /// (for the restart-failure unfreeze test). The record happens BEFORE the failure so the sequence
    /// shows the restart was attempted.
    struct RecordingService {
        ev: Events,
        fail: bool,
    }
    impl ServiceManager for RecordingService {
        fn restart(&self, _unit: &str) -> Result<(), ServiceError> {
            record(&self.ev, "restart");
            if self.fail {
                Err(ServiceError::CommandFailed {
                    tool: "mock".to_owned(),
                    status: "1".to_owned(),
                    stderr: "forced restart failure".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    struct AlwaysYes;
    impl Confirm for AlwaysYes {
        fn confirm(&mut self, _s: &str) -> bool {
            true
        }
    }
    struct AlwaysNo;
    impl Confirm for AlwaysNo {
        fn confirm(&mut self, _s: &str) -> bool {
            false
        }
    }

    /// A temp dir helper (no tempfile dep; mirrors persist.rs's pattern).
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ic-upgrade-orch-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// Build an `UpgradeArgs` for the orchestration tests. The SAVE step and the health gate are
    /// BOTH injected as mocks, so these loopback addresses are never actually dialed; they are
    /// placeholders. `--yes` is set so the confirm prompt is skipped by default (the decline test
    /// flips it off).
    fn args_in(dir: &Path, target: &Path, new_bin: &Path) -> UpgradeArgs {
        UpgradeArgs {
            binary: new_bin.to_path_buf(),
            sha256sums: dir.join("SHA256SUMS"),
            target: target.to_path_buf(),
            unit: "ironcache".to_owned(),
            readyz_addr: "127.0.0.1:9121".to_owned(),
            resp_addr: "127.0.0.1:6379".to_owned(),
            auth: None,
            health_timeout: Duration::from_millis(50),
            no_rollback: false,
            yes: true,
            allow_same: false,
            no_freeze: false,
        }
    }

    /// Write a tiny Unix shell-script "binary" that answers `--version` with `version` (and is a
    /// no-op otherwise). This is a REAL runnable program so `verify::probe_binary_version` works, but
    /// without baking the test on the test harness binary's flags. Mode 0755.
    #[cfg(unix)]
    fn write_fake_binary(path: &Path, version: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        let body = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo \"ironcache {version}\"; fi\nexit 0\n"
        );
        // `fs::write` opens, writes, and CLOSES the fd before returning, so the test holds NO writer
        // fd by the time `run_with` exec's the binary -- the ETXTBSY race a lingering writer would
        // cause is avoided test-side, and the `proc::run_bounded` retry covers any residual kernel-side
        // ETXTBSY. (Do NOT switch this to a held `File` handle: that would re-introduce the writer.)
        std::fs::write(path, body).expect("write fake binary");
        let mut perm = std::fs::metadata(path).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(path, perm).unwrap();
    }

    /// The swap's `.new` staging path (`<target>.new`) -- must NOT exist after a successful swap (the
    /// rename consumed it). Mirrors `swap::new_path` without exposing it.
    fn staged_new(target: &Path) -> PathBuf {
        let mut name = target.file_name().unwrap().to_os_string();
        name.push(".new");
        target.parent().unwrap().join(name)
    }

    #[cfg(unix)]
    #[test]
    fn healthy_probe_keeps_new_binary_and_old_slot() {
        let dir = temp_dir("healthy");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        // Distinct versions: current=1.0.0, new=2.0.0 (a real upgrade, no --allow-same needed).
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin);
        let out = run_with(&a, deps, &mut AlwaysYes).expect("healthy upgrade succeeds");
        assert_eq!(
            out.installed_version, "2.0.0",
            "the new version is installed"
        );
        assert_eq!(
            out.previous_version.as_deref(),
            Some("1.0.0"),
            "prior version recorded"
        );
        assert!(
            old_path_of(&target).exists(),
            "the .old rollback slot is retained on success"
        );
        assert!(target.exists(), "the target binary is in place");
        assert!(
            !staged_new(&target).exists(),
            "the .new staging file was consumed by the swap rename"
        );
        // The target now reports the NEW version (the swap moved the new bytes into place).
        assert_eq!(
            crate::upgrade::verify::probe_binary_version(&target).unwrap(),
            "2.0.0"
        );
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            1,
            "one restart on the happy path"
        );
        assert!(
            out.save_confirmed,
            "the mock saver confirmed a current snapshot before the swap (data-safe)"
        );
    }

    /// `old_path` is internal to `swap`; recompute the `.old` slot path the same way for assertions.
    fn old_path_of(target: &Path) -> PathBuf {
        let mut name = target.file_name().unwrap().to_os_string();
        name.push(".old");
        target.parent().unwrap().join(name)
    }

    #[cfg(unix)]
    #[test]
    fn unhealthy_probe_auto_rolls_back() {
        let dir = temp_dir("unhealthy");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        // Capture the original (v1) target bytes so we can assert the rollback restored them.
        let original = std::fs::read(&target).expect("read original target");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            // First gate (post-swap) FAILS -> rollback; the rollback re-probe (full gate, prev
            // version known) then succeeds.
            probe: MockProbe::new(vec![Err("readyz never 200".to_owned()), Ok(())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("unhealthy upgrade fails");
        match &err {
            UpgradeError::HealthGate { rollback, .. } => {
                assert!(
                    rollback.contains("back and healthy"),
                    "rollback outcome reported: {rollback}"
                );
            }
            other => panic!("expected HealthGate, got {other:?}"),
        }
        // The restored target equals the original v1 bytes (rollback put .old back).
        let restored = std::fs::read(&target).expect("read restored target");
        assert_eq!(
            restored, original,
            "rollback restored the prior binary bytes"
        );
        assert_eq!(
            crate::upgrade::verify::probe_binary_version(&target).unwrap(),
            "1.0.0",
            "the restored binary reports the prior version"
        );
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            2,
            "one restart for the swap, one for the rollback"
        );
    }

    #[cfg(unix)]
    #[test]
    fn no_rollback_leaves_new_binary_and_reports_failure() {
        let dir = temp_dir("norollback");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Err("version mismatch".to_owned())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.no_rollback = true;
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("fails");
        match &err {
            UpgradeError::HealthGate { rollback, .. } => {
                assert!(
                    rollback.contains("--no-rollback"),
                    "no-rollback noted: {rollback}"
                );
            }
            other => panic!("expected HealthGate, got {other:?}"),
        }
        // The new binary is LEFT in place (we did not roll back); the .old slot still exists.
        assert_eq!(
            crate::upgrade::verify::probe_binary_version(&target).unwrap(),
            "2.0.0",
            "the new (failed) binary is left in place under --no-rollback"
        );
        assert!(old_path_of(&target).exists(), "the .old slot still exists");
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            1,
            "only the swap restart; no rollback restart"
        );
    }

    #[cfg(unix)]
    #[test]
    fn declining_the_prompt_aborts_before_any_change() {
        let dir = temp_dir("decline");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.yes = false; // so the prompt is asked
        let err = run_with(&a, deps, &mut AlwaysNo).expect_err("declined");
        assert!(matches!(err, UpgradeError::Aborted), "{err:?}");
        assert!(
            !old_path_of(&target).exists(),
            "no swap happened (no .old slot) when the operator declined"
        );
        assert_eq!(restarts.load(Ordering::SeqCst), 0, "no restart on abort");
    }

    /// A same-version target without --allow-same/--yes is refused (a likely mistake).
    #[cfg(unix)]
    #[test]
    fn same_version_without_allow_same_is_refused() {
        let dir = temp_dir("same");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "3.3.3");
        write_fake_binary(&new_bin, "3.3.3"); // same version
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.yes = false; // not bypassed
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("same version refused");
        assert!(matches!(err, UpgradeError::SameVersion { .. }), "{err:?}");
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            0,
            "no restart: refused before the swap"
        );
        assert!(!old_path_of(&target).exists(), "no swap happened");
    }

    /// CRITICAL fix #3: when the readyz pre-flight fails (nothing listening), the upgrade aborts
    /// BEFORE any on-disk change -- it does not swap-then-rollback a healthy binary.
    #[cfg(unix)]
    #[test]
    fn readyz_preflight_failure_aborts_before_swap() {
        let dir = temp_dir("preflight");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let original = std::fs::read(&target).unwrap();
        let restarts = Arc::new(AtomicUsize::new(0));
        let mut probe = MockProbe::new(vec![Ok(())]);
        probe.preflight_ok = false; // nothing listening on the readyz addr
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe,
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("preflight failure aborts");
        assert!(
            matches!(err, UpgradeError::ReadyzPreflight { .. }),
            "{err:?}"
        );
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            0,
            "no restart on a preflight abort"
        );
        assert!(!old_path_of(&target).exists(), "no swap happened");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            original,
            "the target binary is untouched after a preflight abort"
        );
    }

    /// MEDIUM fix #8: a PRESENT-but-UNRUNNABLE existing target is NOT silently treated as a fresh
    /// install (which would skip the same-version guard). We assert the upgrade proceeds (current
    /// version unknown) and the outcome records previous_version = None, while a real upgrade to a
    /// different version still happens. The key behavior is that an unrunnable EXISTING binary does
    /// not get mistaken for "no binary".
    #[cfg(unix)]
    #[test]
    fn present_but_unrunnable_current_binary_is_not_a_fresh_install() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir("unrunnable");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        // The current target is present but NOT executable / not a valid program (cannot report a
        // version). The new binary is a valid versioned script.
        std::fs::write(&target, b"not a runnable program").unwrap();
        let mut perm = std::fs::metadata(&target).unwrap().permissions();
        perm.set_mode(0o644); // not executable -> --version cannot run
        std::fs::set_permissions(&target, perm).unwrap();
        write_fake_binary(&new_bin, "2.0.0");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: confirming_saver(),
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin);
        let out = run_with(&a, deps, &mut AlwaysYes).expect("upgrade proceeds");
        assert_eq!(out.installed_version, "2.0.0");
        // current_version is None (we could not read the present-but-unrunnable binary), but the
        // swap still preserved the OLD bytes in .old (proving it was treated as an existing target,
        // not a fresh install that would skip preserving a slot).
        assert!(
            out.previous_version.is_none(),
            "unrunnable current -> version unknown"
        );
        assert!(
            old_path_of(&target).exists(),
            "the present (unrunnable) binary was preserved in .old, NOT treated as a fresh install"
        );
    }

    /// No persistence configured: WITHOUT --yes the upgrade refuses (honest data-loss guard);
    /// WITH --yes it proceeds (save_confirmed = false).
    #[cfg(unix)]
    #[test]
    fn no_persistence_requires_yes() {
        let dir = temp_dir("nopersist");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let no_persist = || MockSaver {
            outcome: save::SaveOutcome::NoPersistence {
                reason: "persistence disabled".to_owned(),
            },
        };

        // Without --yes -> refused before any swap.
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: no_persist(),
            pauser: noop_pauser(),
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.yes = false;
        // The operator CONFIRMS the prompt (AlwaysYes), but `yes=false` means the no-persistence
        // data-loss gate is NOT bypassed, so SAVE-first's NoPersistence outcome aborts the upgrade.
        let err =
            run_with(&a, deps, &mut AlwaysYes).expect_err("no persistence + no --yes refuses");
        assert!(matches!(err, UpgradeError::NoPersistence { .. }), "{err:?}");
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            0,
            "refused before the swap"
        );
        assert!(!old_path_of(&target).exists(), "no swap happened");

        // With --yes -> proceeds, save_confirmed = false (the honest data-loss acknowledgement).
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: no_persist(),
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin); // yes = true
        let out = run_with(&a, deps, &mut AlwaysYes).expect("with --yes it proceeds");
        assert!(
            !out.save_confirmed,
            "no persistence -> save not confirmed under --yes"
        );
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            1,
            "the swap restart happened"
        );
    }

    /// A SAVE that errors (connect/auth/protocol) is FATAL even under --yes: we never swap over a
    /// save we could not reason about.
    #[cfg(unix)]
    #[test]
    fn fatal_save_error_aborts_even_with_yes() {
        struct ErringSaver;
        impl Saver for ErringSaver {
            fn save_first(&self, _t: &SaveTarget) -> Result<save::SaveOutcome, SaveError> {
                Err(SaveError::Connect {
                    addr: "127.0.0.1:6379".to_owned(),
                    detail: "connection refused".to_owned(),
                })
            }
        }
        let dir = temp_dir("savefail");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let restarts = Arc::new(AtomicUsize::new(0));
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: MockService {
                restarts: Arc::clone(&restarts),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: ErringSaver,
            pauser: noop_pauser(),
        };
        let a = args_in(&dir, &target, &new_bin); // yes = true, still fatal
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a save error is fatal");
        assert!(matches!(err, UpgradeError::Save { .. }), "{err:?}");
        assert_eq!(
            restarts.load(Ordering::SeqCst),
            0,
            "no swap over an unconfirmed save"
        );
        assert!(!old_path_of(&target).exists(), "no swap happened");
    }

    /// ORCHESTRATION ORDER (#388): with recording mocks, the freeze (PAUSE) happens BEFORE the SAVE,
    /// which happens BEFORE the swap/restart. The recorded event sequence proves the lossless order.
    #[cfg(unix)]
    #[test]
    fn freeze_happens_before_save_which_is_before_restart() {
        let dir = temp_dir("order");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let a = args_in(&dir, &target, &new_bin);
        run_with(&a, deps, &mut AlwaysYes).expect("healthy upgrade with freeze succeeds");
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause", "save", "restart"],
            "the lossless order is freeze -> save -> restart (no unpause on the happy path)"
        );
    }

    /// `--no-freeze` SKIPS the pause entirely: the recorded sequence is SAVE then restart, with NO
    /// pause/unpause -- exactly v1's SAVE-first-only behavior.
    #[cfg(unix)]
    #[test]
    fn no_freeze_skips_the_pause() {
        let dir = temp_dir("nofreeze");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            // A pauser that would PANIC if freeze/unfreeze were ever called -- proving --no-freeze
            // never touches it.
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.no_freeze = true;
        run_with(&a, deps, &mut AlwaysYes).expect("no-freeze upgrade succeeds");
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["save", "restart"],
            "--no-freeze: SAVE-first only, no pause, no unpause"
        );
    }

    /// ABORT AFTER PAUSE (#388): a SAVE that fails AFTER the freeze must UNPAUSE the still-living old
    /// process, so production is never left write-frozen. The recorded sequence is pause -> save ->
    /// unpause, and NO restart happens (no swap over a failed save).
    #[cfg(unix)]
    #[test]
    fn abort_after_pause_issues_unpause() {
        let dir = temp_dir("abortunpause");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: true, // the SAVE fails AFTER the freeze
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a save failure after the freeze");
        match &err {
            UpgradeError::Save { unfreeze, .. } => assert!(
                unfreeze.contains("CLIENT UNPAUSE"),
                "the unfreeze outcome is recorded on the error: {unfreeze}"
            ),
            other => panic!("expected Save, got {other:?}"),
        }
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause", "save", "unpause"],
            "abort after the freeze: pause -> failed save -> unpause; NO restart"
        );
    }

    /// A FREEZE that fails is FATAL and aborts BEFORE the SAVE / any on-disk change (the old process
    /// was never frozen, so there is nothing to unpause). `--no-freeze` is the escape hatch.
    #[cfg(unix)]
    #[test]
    fn freeze_failure_is_fatal_before_save() {
        let dir = temp_dir("freezefail");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: true, // the freeze fails
            },
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a freeze failure is fatal");
        assert!(matches!(err, UpgradeError::Freeze(_)), "{err:?}");
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause"],
            "the freeze was ATTEMPTED then failed; no save, no restart, no unpause (never frozen)"
        );
        assert!(!old_path_of(&target).exists(), "no swap happened");
    }

    /// No persistence under `--yes`: the freeze is engaged first (we cannot know persistence is off
    /// until the SAVE), but since the upgrade PROCEEDS to the restart, the old process dies and the
    /// freeze evaporates -- NO unpause is issued (the recorded sequence is pause -> save -> restart).
    #[cfg(unix)]
    #[test]
    fn no_persistence_with_yes_proceeds_without_unpause() {
        let dir = temp_dir("nopersistyes");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::NoPersistence {
                    reason: "persistence disabled".to_owned(),
                },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let a = args_in(&dir, &target, &new_bin); // yes = true
        let out = run_with(&a, deps, &mut AlwaysYes).expect("proceeds under --yes");
        assert!(!out.save_confirmed, "no persistence -> save not confirmed");
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause", "save", "restart"],
            "proceeds to the restart; the dying old process clears the freeze (no unpause)"
        );
    }

    /// Make `target` a SYMLINK so `swap::swap` fails immediately with `SymlinkTarget` (a deterministic
    /// swap failure that does not touch the filesystem), to exercise the swap-failure abort path.
    #[cfg(unix)]
    fn make_symlink_target(dir: &Path, target: &Path) {
        // Point the symlink at some existing file in the dir; the swap refuses it as a symlink before
        // ever following it.
        let dangling = dir.join("link-dest");
        std::fs::write(&dangling, b"x").unwrap();
        std::os::unix::fs::symlink(&dangling, target).unwrap();
    }

    /// SWAP-FAILURE AFTER THE FREEZE (#388 review): the freeze persists past `freeze_save_preflight`
    /// (it must, to survive swap+restart), but a swap FAILURE returns while the old (frozen) process
    /// is still alive. The orchestrator must UNPAUSE it -- a failed swap never leaves prod write-
    /// frozen. The recorded sequence is pause -> save -> unpause (NO restart: the swap failed), and the
    /// error is a `Swap` carrying the unfreeze note.
    #[cfg(unix)]
    #[test]
    fn swap_failure_after_freeze_issues_unpause() {
        let dir = temp_dir("swapfail");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&new_bin, "2.0.0");
        // target is a SYMLINK -> swap::swap refuses it -> SwapError::SymlinkTarget.
        make_symlink_target(&dir, &target);
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a swap failure");
        match &err {
            UpgradeError::Swap { unfreeze, .. } => assert!(
                unfreeze.contains("CLIENT UNPAUSE"),
                "the swap error carries the unfreeze outcome: {unfreeze}"
            ),
            other => panic!("expected Swap, got {other:?}"),
        }
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause", "save", "unpause"],
            "swap failure after the freeze: pause -> save -> (swap fails) -> unpause; NO restart"
        );
    }

    /// RESTART-FAILURE AFTER THE FREEZE (#388 review): the restart is the step that kills the old
    /// (frozen) process; if it FAILS the old process may still be alive AND frozen, so the orchestrator
    /// UNPAUSEs it (best effort). The recorded sequence is pause -> save -> restart -> unpause, and the
    /// error is a `Restart` carrying the unfreeze note.
    #[cfg(unix)]
    #[test]
    fn restart_failure_after_freeze_issues_unpause() {
        let dir = temp_dir("restartfail");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: true, // the restart fails AFTER the swap
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let a = args_in(&dir, &target, &new_bin);
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a restart failure");
        match &err {
            UpgradeError::Restart { unfreeze, .. } => assert!(
                unfreeze.contains("CLIENT UNPAUSE"),
                "the restart error carries the unfreeze outcome: {unfreeze}"
            ),
            other => panic!("expected Restart, got {other:?}"),
        }
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["pause", "save", "restart", "unpause"],
            "restart failure after the freeze: pause -> save -> restart (fails) -> unpause"
        );
    }

    /// Under `--no-freeze`, a swap failure does NOT unpause (no freeze was ever set). The recorded
    /// sequence is just save (the swap fails, no pause, no unpause), and the error is a bare `Swap`.
    #[cfg(unix)]
    #[test]
    fn swap_failure_with_no_freeze_does_not_unpause() {
        let dir = temp_dir("swapfailnofreeze");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&new_bin, "2.0.0");
        make_symlink_target(&dir, &target);
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: false,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.no_freeze = true;
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a swap failure");
        match &err {
            UpgradeError::Swap { unfreeze, .. } => assert!(
                unfreeze.is_empty(),
                "--no-freeze: no unfreeze note on the swap error: {unfreeze}"
            ),
            other => panic!("expected Swap, got {other:?}"),
        }
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["save"],
            "--no-freeze swap failure: SAVE only, no pause, no unpause"
        );
    }

    /// Under `--no-freeze`, a restart failure does NOT unpause. The recorded sequence is save ->
    /// restart (no pause, no unpause), and the error is a bare `Restart`.
    #[cfg(unix)]
    #[test]
    fn restart_failure_with_no_freeze_does_not_unpause() {
        let dir = temp_dir("restartfailnofreeze");
        let target = dir.join("ironcache");
        let new_bin = dir.join("ironcache.new-src");
        write_fake_binary(&target, "1.0.0");
        write_fake_binary(&new_bin, "2.0.0");
        let ev = events();
        let deps = UpgradeDeps {
            source: FixedSource {
                path: new_bin.clone(),
                name: "ironcache".to_owned(),
            },
            verifier: PassVerifier,
            service: RecordingService {
                ev: Arc::clone(&ev),
                fail: true,
            },
            probe: MockProbe::new(vec![Ok(())]),
            saver: RecordingSaver {
                ev: Arc::clone(&ev),
                outcome: save::SaveOutcome::Confirmed { last_save: 1 },
                err: false,
            },
            pauser: RecordingPauser {
                ev: Arc::clone(&ev),
                freeze_err: false,
            },
        };
        let mut a = args_in(&dir, &target, &new_bin);
        a.no_freeze = true;
        let err = run_with(&a, deps, &mut AlwaysYes).expect_err("a restart failure");
        match &err {
            UpgradeError::Restart { unfreeze, .. } => assert!(
                unfreeze.is_empty(),
                "--no-freeze: no unfreeze note on the restart error: {unfreeze}"
            ),
            other => panic!("expected Restart, got {other:?}"),
        }
        let seq = ev.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec!["save", "restart"],
            "--no-freeze restart failure: SAVE then restart, no pause, no unpause"
        );
    }

    #[test]
    fn read_auth_file_trims_trailing_newline() {
        let dir = temp_dir("auth");
        let f = dir.join("pw");
        std::fs::write(&f, "s3cr3t\n").unwrap();
        let pw = read_auth_file(Some(&f)).unwrap();
        assert_eq!(pw.as_deref(), Some("s3cr3t"));
        // An empty file -> None.
        std::fs::write(&f, "\n").unwrap();
        assert_eq!(read_auth_file(Some(&f)).unwrap(), None);
        // No path -> None.
        assert_eq!(read_auth_file(None).unwrap(), None);
        // A missing file -> typed error, not a panic.
        let missing = dir.join("nope");
        assert!(matches!(
            read_auth_file(Some(&missing)),
            Err(UpgradeError::AuthFile { .. })
        ));
    }
}
