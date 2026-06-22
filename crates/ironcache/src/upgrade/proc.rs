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
    use std::process::Stdio;
    let prog_name = program.display().to_string();
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| BoundedError::Spawn {
            program: prog_name.clone(),
            detail: e.to_string(),
        })?;

    // Poll for exit under a monotonic deadline. The wait/sleep runs on a throwaway current-thread
    // runtime so the inter-poll delay goes through the Runtime timer seam (not std::time).
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            // We already spawned; kill it so we do not leak a child, then surface the error.
            let _ = child.kill();
            let _ = child.wait();
            return Err(BoundedError::Wait {
                program: prog_name,
                detail: format!("could not build the wait runtime: {e}"),
            });
        }
    };

    let exited = rt.block_on(wait_with_deadline(&mut child, timeout, &prog_name));
    match exited {
        Ok(true) => {
            // The child has exited; collect its output (status + captured streams).
            child.wait_with_output().map_err(|e| BoundedError::Wait {
                program: prog_name,
                detail: e.to_string(),
            })
        }
        Ok(false) => {
            // Deadline elapsed: kill + reap so we never leave a zombie / leaked child, then report.
            let _ = child.kill();
            let _ = child.wait();
            Err(BoundedError::Timeout {
                program: prog_name,
                timeout,
            })
        }
        Err(detail) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(BoundedError::Wait {
                program: prog_name,
                detail,
            })
        }
    }
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
}
