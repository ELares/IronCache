// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded subprocess execution for `ironcache upgrade` (MEDIUM review fix #7).
//!
//! Both `<new-binary> --version` (which runs the UNTRUSTED new binary) and `systemctl ...` are
//! external processes that could HANG; a bare `Command::output()` would wedge the upgrade forever. A
//! hung child must become a TYPED error, not a hang. [`run_bounded`] spawns the command with piped
//! stdio, polls [`std::process::Child::try_wait`] until the child exits OR a deadline elapses, and on
//! timeout KILLs the child and returns [`BoundedError::Timeout`].
//!
//! ## Determinism seam
//!
//! The deadline is measured through the `ironcache-env` monotonic clock seam (ADR-0003), and the
//! inter-poll sleep is a `Runtime` timer await driven on a throwaway current-thread runtime, so no
//! `std::time::Instant`/`SystemTime` is read directly (the invariant lint is satisfied). This is the
//! short-lived operator CLI, not the server, and `Command` is NOT a `fork` syscall (no-fork
//! invariant 4 is about the server).

use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

/// The cadence at which a bounded run polls the child for exit. Small enough to return promptly when
/// the child finishes, large enough not to busy-spin.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The back-off between ETXTBSY spawn retries (see [`spawn_with_etxtbsy_retry`]). ETXTBSY clears the
/// instant the writer closes its fd, so a short wait suffices.
const ETXTBSY_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// How many times to RETRY a spawn that fails with `ETXTBSY` ("Text file busy") before giving up.
/// `execve()` returns `ETXTBSY` when the target file still has a writer fd open -- which is exactly
/// the case for a binary we (or a downstream auto-fetch, #394) JUST wrote/downloaded and are about to
/// run, when the writer's `close()` races the `exec()`. It is TRANSIENT (it clears as soon as the
/// writer closes), so a brief retry loop makes the version probe robust for a freshly written binary.
/// ~10 attempts over ~1s (`ETXTBSY_RETRY_BACKOFF * 10`) is generous; other spawn errors are NOT
/// retried (they are not transient).
const ETXTBSY_MAX_RETRIES: u32 = 10;

/// A bounded-run failure: the child could not be spawned, an IO error occurred reading its output, or
/// it did not exit within the deadline (and was killed).
#[derive(Debug, thiserror::Error)]
pub enum BoundedError {
    /// The command could not be spawned at all.
    #[error("could not spawn {program}: {detail}")]
    Spawn {
        /// The program path / name.
        program: String,
        /// The OS error.
        detail: String,
    },
    /// The child did not exit within the deadline and was killed.
    #[error("{program} did not exit within {timeout:?} (killed)")]
    Timeout {
        /// The program path / name.
        program: String,
        /// The deadline that elapsed.
        timeout: Duration,
    },
    /// An IO error while waiting for / collecting the child's output.
    #[error("waiting for {program}: {detail}")]
    Wait {
        /// The program path / name.
        program: String,
        /// The OS error.
        detail: String,
    },
}

/// Run `program` with `args`, capturing stdout/stderr, bounded by `timeout`. Returns the process
/// [`Output`] (status + captured streams) on a normal exit; a [`BoundedError::Timeout`] (after
/// killing the child) if it does not exit in time; or a spawn/wait error.
///
/// # Errors
///
/// Returns a [`BoundedError`] on spawn failure, timeout, or a wait/IO error.
pub fn run_bounded(
    program: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<Output, BoundedError> {
    // Run the whole runtime-driven body on a DEDICATED OS thread. `run_bounded` is a SYNC function
    // that internally `block_on`s a throwaway current-thread runtime, but it is reachable from WITHIN
    // a caller's tokio runtime (the health gate `block_on`s `poll_until_healthy`, which calls the
    // version probe). `block_on` panics ("Cannot start a runtime from within a runtime") if the
    // current thread is already driving a runtime, so we isolate it onto its own thread, where no
    // ambient runtime exists. `std::thread::scope` lets the closure borrow `program`/`args` without
    // `'static` bounds and joins before returning. (This is the short-lived operator CLI; one extra
    // thread per bounded subprocess is immaterial, and it is NOT a `fork` syscall, invariant 4.)
    let prog_name = program.display().to_string();
    std::thread::scope(|scope| {
        scope
            .spawn(|| run_bounded_on_this_thread(program, args, timeout, &prog_name))
            .join()
            .unwrap_or_else(|_| {
                // The worker thread panicked (it should not -- the body returns typed errors); surface
                // it as a Wait error rather than propagating the panic across the scope boundary.
                Err(BoundedError::Wait {
                    program: prog_name.clone(),
                    detail: "the bounded-run worker thread panicked".to_owned(),
                })
            })
    })
}

/// The runtime-driven body of [`run_bounded`], always invoked on a dedicated thread with NO ambient
/// tokio runtime, so its `block_on` calls never nest. Builds one throwaway current-thread runtime,
/// spawns the child (with the ETXTBSY retry), and waits for it under the deadline.
fn run_bounded_on_this_thread(
    program: &Path,
    args: &[&str],
    timeout: Duration,
    prog_name: &str,
) -> Result<Output, BoundedError> {
    // Build the throwaway current-thread runtime FIRST: it backs both the ETXTBSY spawn-retry back-off
    // and the exit-poll wait, so both inter-attempt delays go through the Runtime timer seam (not
    // std::time). Built before the spawn so a spawn that needs to retry already has its timer.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| BoundedError::Wait {
            program: prog_name.to_owned(),
            detail: format!("could not build the wait runtime: {e}"),
        })?;

    // SPAWN with a bounded ETXTBSY retry: a binary we (or #394's auto-fetch) just wrote can still have
    // a writer fd open, so execve() races into ETXTBSY ("Text file busy"); it clears the instant the
    // writer closes. Only ETXTBSY is retried; any other spawn error fails immediately.
    let mut child = rt.block_on(spawn_with_etxtbsy_retry(program, args, prog_name))?;

    let exited = rt.block_on(wait_with_deadline(&mut child, timeout, prog_name));
    match exited {
        Ok(true) => {
            // The child has exited; collect its output (status + captured streams).
            child.wait_with_output().map_err(|e| BoundedError::Wait {
                program: prog_name.to_owned(),
                detail: e.to_string(),
            })
        }
        Ok(false) => {
            // Deadline elapsed: kill + reap so we never leave a zombie / leaked child, then report.
            let _ = child.kill();
            let _ = child.wait();
            Err(BoundedError::Timeout {
                program: prog_name.to_owned(),
                timeout,
            })
        }
        Err(detail) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(BoundedError::Wait {
                program: prog_name.to_owned(),
                detail,
            })
        }
    }
}

/// Spawn `program` with piped stdio, RETRYING on `ETXTBSY` ("Text file busy") up to
/// [`ETXTBSY_MAX_RETRIES`] times with a [`ETXTBSY_RETRY_BACKOFF`] back-off between attempts (via the
/// Runtime timer seam). `ETXTBSY` is the transient race where a binary still has a writer fd open at
/// the moment of `execve()` (a just-written / just-downloaded binary whose writer's `close()` has not
/// yet landed); it clears as soon as the writer closes, so a brief retry loop makes the exec robust.
/// Any OTHER spawn error fails immediately (it is not transient). On exhausting the budget the LAST
/// `ETXTBSY` error is returned as a typed [`BoundedError::Spawn`].
async fn spawn_with_etxtbsy_retry(
    program: &Path,
    args: &[&str],
    prog_name: &str,
) -> Result<std::process::Child, BoundedError> {
    use ironcache_runtime::Runtime as _;
    use std::process::Stdio;
    let rt = ironcache_runtime::TokioRuntime::new();
    let mut attempt: u32 = 0;
    loop {
        match Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => return Ok(child),
            Err(e) if is_etxtbsy(&e) && attempt < ETXTBSY_MAX_RETRIES => {
                // Transient: the writer fd is still open. Back off (through the timer seam) and retry.
                attempt += 1;
                rt.timer(ETXTBSY_RETRY_BACKOFF).await;
            }
            Err(e) => {
                return Err(BoundedError::Spawn {
                    program: prog_name.to_owned(),
                    detail: e.to_string(),
                });
            }
        }
    }
}

/// Whether a spawn IO error is `ETXTBSY` ("Text file busy", `execve` against a file with an open
/// writer fd). Matches the dedicated `ErrorKind::ExecutableFileBusy` (stable since Rust 1.83) AND the
/// raw OS number (`libc::ETXTBSY`) as a belt-and-suspenders fallback in case the kind is not mapped on
/// some target.
fn is_etxtbsy(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::ExecutableFileBusy || e.raw_os_error() == Some(libc::ETXTBSY)
}

/// Poll `child.try_wait()` until it reports an exit (`Ok(true)`) or the monotonic `timeout` elapses
/// (`Ok(false)`). A `try_wait` IO error is `Err(detail)`. Elapsed time uses the env monotonic clock;
/// the sleep uses the Runtime timer seam.
async fn wait_with_deadline(
    child: &mut std::process::Child,
    timeout: Duration,
    _prog_name: &str,
) -> Result<bool, String> {
    use ironcache_env::{Clock as _, SystemEnv};
    use ironcache_runtime::Runtime as _;
    let env = SystemEnv::new();
    let start = env.now();
    let rt = ironcache_runtime::TokioRuntime::new();
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => return Ok(true),
            Ok(None) => {} // still running
            Err(e) => return Err(e.to_string()),
        }
        if env.now().saturating_duration_since(start) >= timeout {
            return Ok(false);
        }
        rt.timer(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ic-upgrade-proc-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[cfg(unix)]
    fn write_script(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(path, body).unwrap();
        let mut perm = std::fs::metadata(path).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(path, perm).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn fast_command_returns_output() {
        let dir = temp_dir("fast");
        let script = dir.join("echo-version");
        write_script(&script, "#!/bin/sh\necho 'ironcache 1.2.3'\nexit 0\n");
        let out = run_bounded(&script, &["--version"], Duration::from_secs(5)).expect("runs");
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("1.2.3"));
    }

    #[cfg(unix)]
    #[test]
    fn hung_command_times_out_and_is_killed() {
        let dir = temp_dir("hang");
        let script = dir.join("hang");
        // Sleep far longer than the deadline; the bounded run must kill it and return Timeout.
        write_script(&script, "#!/bin/sh\nsleep 30\n");
        let err = run_bounded(&script, &[], Duration::from_millis(300)).expect_err("times out");
        assert!(matches!(err, BoundedError::Timeout { .. }), "{err:?}");
    }

    #[test]
    fn unspawnable_is_a_typed_error() {
        let err = run_bounded(
            Path::new("/this/does/not/exist/ironcache"),
            &["--version"],
            Duration::from_secs(1),
        )
        .expect_err("cannot spawn");
        assert!(matches!(err, BoundedError::Spawn { .. }), "{err:?}");
    }

    #[cfg(unix)]
    #[test]
    fn nonzero_exit_is_returned_as_output_not_error() {
        let dir = temp_dir("nonzero");
        let script = dir.join("fail");
        write_script(&script, "#!/bin/sh\nexit 7\n");
        let out = run_bounded(&script, &[], Duration::from_secs(5)).expect("runs (nonzero is Ok)");
        assert_eq!(
            out.status.code(),
            Some(7),
            "the nonzero status is surfaced in Output"
        );
    }

    /// `is_etxtbsy` recognizes the ETXTBSY error by BOTH the dedicated `ExecutableFileBusy` kind and
    /// the raw OS number, and does NOT match an unrelated error.
    #[test]
    fn is_etxtbsy_detects_text_file_busy() {
        // The dedicated kind (stable since 1.83).
        let by_kind = std::io::Error::from(std::io::ErrorKind::ExecutableFileBusy);
        assert!(is_etxtbsy(&by_kind), "ExecutableFileBusy kind is ETXTBSY");
        // The raw OS number (the belt-and-suspenders fallback).
        let by_raw = std::io::Error::from_raw_os_error(libc::ETXTBSY);
        assert!(
            is_etxtbsy(&by_raw),
            "raw os error {} is ETXTBSY",
            libc::ETXTBSY
        );
        // An unrelated error is NOT ETXTBSY (so it is never retried).
        let other = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert!(!is_etxtbsy(&other), "NotFound is not ETXTBSY");
    }

    /// THE ETXTBSY-ROBUSTNESS PROOF (#388 flake fix): a binary EXEC'd immediately after it was
    /// written + chmod'd must still run. Spawning right after the write is exactly the window where
    /// `execve()` can race a still-open writer fd into `ETXTBSY`; the bounded retry inside `run_bounded`
    /// must absorb that. Repeated in a tight loop so a transient ETXTBSY under parallel load is hit and
    /// recovered (rather than failing the test the way the original probe did at mod.rs:1251).
    #[cfg(unix)]
    #[test]
    fn run_bounded_succeeds_on_a_freshly_written_binary() {
        let dir = temp_dir("freshexec");
        for i in 0..25 {
            let script = dir.join(format!("fresh-{i}"));
            // Write + chmod, then IMMEDIATELY run (no intervening delay) -- the ETXTBSY window.
            write_script(&script, "#!/bin/sh\necho 'ironcache 9.9.9'\nexit 0\n");
            let out = run_bounded(&script, &["--version"], Duration::from_secs(5))
                .expect("a freshly written binary runs (ETXTBSY retry absorbs the race)");
            assert!(out.status.success(), "iter {i}: exited cleanly");
            assert!(
                String::from_utf8_lossy(&out.stdout).contains("9.9.9"),
                "iter {i}: version printed"
            );
        }
    }

    /// REGRESSION GUARD: `run_bounded` is reachable from WITHIN a tokio runtime (the health gate
    /// `block_on`s `poll_until_healthy`, which calls the version probe on a SUCCESSFULLY-spawning
    /// binary). Its internal `block_on` must not panic with "Cannot start a runtime from within a
    /// runtime" -- it runs on a dedicated thread to avoid nesting. This asserts a SUCCESSFUL run works
    /// from inside a current-thread runtime (the exact prod gate path, and the path the ETXTBSY change
    /// first tripped on the spawn-failure case).
    #[cfg(unix)]
    #[test]
    fn run_bounded_works_from_within_a_tokio_runtime() {
        let dir = temp_dir("inruntime");
        let script = dir.join("ver");
        write_script(&script, "#!/bin/sh\necho 'ironcache 4.5.6'\nexit 0\n");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async {
            // Calling the SYNC run_bounded from inside an async block on this runtime: the old inline
            // block_on would panic here; the dedicated-thread isolation makes it work.
            run_bounded(&script, &["--version"], Duration::from_secs(5))
        });
        let out = out.expect("run_bounded works from within a runtime");
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("4.5.6"));
    }
}
