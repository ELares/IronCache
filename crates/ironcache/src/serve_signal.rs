// SPDX-License-Identifier: MIT OR Apache-2.0
//! Signal handling for the server binary: the #139 graceful shutdown (SIGINT/SIGTERM) and the
//! #638 SIGUSR1 streamed live-cutover trigger. Extracted verbatim from `serve.rs` as a cohesive,
//! self-contained group; `serve` re-exports [`wait_for_signal`] and [`SignalOutcome`] so the `main`
//! seam's `serve::wait_for_signal` / `serve::SignalOutcome` paths resolve unchanged.

use std::sync::Arc;
use std::sync::atomic::Ordering;

/// What a delivered signal asks [`wait_for_signal`] to do: begin the graceful SHUTDOWN
/// (SIGINT/SIGTERM, the unchanged #139 path) or begin a streamed live CUTOVER (SIGUSR1, #638). The
/// caller (`main`) branches on this: `Shutdown` drives the drain + join exactly as before; `Cutover`
/// runs the in-server cutover host and, unless it commits, resumes waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// SIGINT/SIGTERM: initiate the graceful shutdown. [`wait_for_signal`] has ALREADY set the
    /// shutdown flag and armed the second-signal force-exit watcher (byte-identical to the prior
    /// behavior); the caller runs `shutdown_and_join`.
    Shutdown,
    /// SIGUSR1 (#638): initiate a streamed live cutover. The shutdown flag is NOT set here (the
    /// cutover must run BEFORE any flag, so `DRAIN_GRACE` never bounds it); the caller drives the
    /// in-server cutover host and, on a non-commit outcome, loops back to [`wait_for_signal`].
    Cutover,
}

/// Resolve WHICH delivered signal fired into a [`SignalOutcome`] (SIGINT/SIGTERM -> `Shutdown`,
/// SIGUSR1 -> `Cutover`). Extracted from [`wait_for_signal`] so the arm selection is unit-tested
/// DETERMINISTICALLY with plain futures (a `ready` for the one that fires, `pending` for the rest)
/// instead of driving real signals into the test process.
#[cfg(unix)]
pub(crate) async fn resolve_signal<I, T, U>(sigint: I, sigterm: T, sigusr1: U) -> SignalOutcome
where
    I: std::future::Future,
    T: std::future::Future,
    U: std::future::Future,
{
    tokio::select! {
        _ = sigint => SignalOutcome::Shutdown,
        _ = sigterm => SignalOutcome::Shutdown,
        _ = sigusr1 => SignalOutcome::Cutover,
    }
}

/// Apply the resolved [`SignalOutcome`]'s FLAG side effect: a `Shutdown` records the stop request on
/// `flag` (so the shards drain + the save-on-exit watch fires); a `Cutover` leaves `flag` UNTOUCHED
/// (the cutover runs before any shutdown). Split out (no watcher arming here) so the flag behavior is
/// unit-tested without installing a real signal handler.
pub(crate) fn apply_signal_flag(outcome: SignalOutcome, flag: &Arc<std::sync::atomic::AtomicBool>) {
    if matches!(outcome, SignalOutcome::Shutdown) {
        flag.store(true, Ordering::SeqCst);
    }
}

/// Block the calling (main) thread until a signal arrives, returning the [`SignalOutcome`] the caller
/// acts on. SIGINT/SIGTERM behave EXACTLY as before (#139, SHUTDOWN.md): the FIRST such signal sets
/// `flag` so the shard accept loops + the save-on-exit watch begin the GRACEFUL stop, and a SECOND
/// arriving while that stop is in progress ESCALATES to an IMMEDIATE `exit(0)`; the function returns
/// [`SignalOutcome::Shutdown`]. SIGUSR1 (#638) is the streamed live-cutover trigger: it returns
/// [`SignalOutcome::Cutover`] WITHOUT setting `flag` or arming the force-exit watcher (the cutover
/// runs first; only a commit later sets the flag). The signal handler itself does NOT terminate from
/// inside the handler (Redis-faithful: a stop signal becomes a controlled shutdown
/// [redis-sigterm-sigint-graceful-shutdown]); it only records the request via `flag`, and the second
/// signal's force-exit is the deliberate IronCache escalation.
///
/// Uses tokio's signal handling on a small dedicated current-thread runtime (signal handling lives
/// in the binary only, CLI_BINARY.md, so the determinism boundary holds).
#[must_use]
pub fn wait_for_signal(flag: &Arc<std::sync::atomic::AtomicBool>) -> SignalOutcome {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        // No runtime: treat as a shutdown request (the safe, prior-behavior default).
        apply_signal_flag(SignalOutcome::Shutdown, flag);
        return SignalOutcome::Shutdown;
    };
    let outcome = rt.block_on(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                return SignalOutcome::Shutdown;
            };
            let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                return SignalOutcome::Shutdown;
            };
            // SIGUSR1 arms the streamed live-cutover trigger (#638). A failure to install it is
            // NON-FATAL: the cutover trigger is simply unavailable and the graceful-shutdown path
            // (SIGINT/SIGTERM) still works, so fall back to shutdown-only selection.
            if let Ok(mut sigusr1) = signal(SignalKind::user_defined1()) {
                resolve_signal(sigint.recv(), sigterm.recv(), sigusr1.recv()).await
            } else {
                tokio::select! {
                    _ = sigint.recv() => SignalOutcome::Shutdown,
                    _ = sigterm.recv() => SignalOutcome::Shutdown,
                }
            }
        }
        #[cfg(not(unix))]
        {
            // SIGUSR1 is a unix signal; off unix there is only the graceful ctrl-c stop.
            let _ = tokio::signal::ctrl_c().await;
            SignalOutcome::Shutdown
        }
    });
    // Record the stop request ONLY for a Shutdown (a Cutover leaves the flag untouched: it runs
    // before any shutdown so DRAIN_GRACE never bounds it).
    apply_signal_flag(outcome, flag);
    if outcome == SignalOutcome::Cutover {
        // A cutover does NOT arm the force-exit watcher: the OLD keeps serving until (and unless) a
        // commit later sets the flag. Return to the caller, which drives the in-server cutover host.
        return SignalOutcome::Cutover;
    }

    // SECOND-SIGNAL FORCE (#139, SHUTDOWN.md): arm a DEDICATED long-lived watcher thread for the
    // ESCALATION. It must outlive this function (the graceful join the caller runs next can take up
    // to the drain grace window), so it owns its OWN current-thread runtime on its OWN OS thread
    // rather than a task on the short-lived runtime above (which is dropped when this fn returns). A
    // second SIGINT/SIGTERM arriving while the graceful stop is in progress forces an immediate
    // `exit(0)` so an operator can always force the issue. On unix only (the signal surface); a
    // build-failure to install the watcher is non-fatal (the graceful path still completes).
    #[cfg(unix)]
    {
        let _ = std::thread::Builder::new()
            .name("ironcache-force-stop".to_string())
            .spawn(|| {
                let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                rt.block_on(async {
                    use tokio::signal::unix::{SignalKind, signal};
                    let (Ok(mut sigint), Ok(mut sigterm)) = (
                        signal(SignalKind::interrupt()),
                        signal(SignalKind::terminate()),
                    ) else {
                        return;
                    };
                    tokio::select! {
                        _ = sigint.recv() => {}
                        _ = sigterm.recv() => {}
                    }
                    tracing::warn!("ironcache: second stop signal -> forcing immediate exit");
                    std::process::exit(0);
                });
            });
    }
    SignalOutcome::Shutdown
}
