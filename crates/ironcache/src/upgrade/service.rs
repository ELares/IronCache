// SPDX-License-Identifier: MIT OR Apache-2.0
//! The SERVICE-MANAGER seam for `ironcache upgrade`.
//!
//! After the atomic swap, the server process must be restarted onto the new binary. v1 ships
//! [`SystemdManager`] (`systemctl restart <unit>`), matching `packaging/ironcache.service`
//! (`Type=simple`, `Restart=on-failure`). The orchestrator depends only on the [`ServiceManager`]
//! trait, so a non-systemd manager (a direct signal + re-exec for a foreground/dev deployment, or a
//! launchd/openrc adapter) drops in without touching the flow; tests inject a mock.
//!
//! `systemctl` is invoked via [`std::process::Command`] from THIS short-lived, operator-run CLI
//! process. That is NOT a `fork` syscall (the no-fork invariant 4 forbids the SERVER forking; an
//! operator CLI spawning `systemctl` is the sanctioned restart path) and does not run inside the
//! server.

use std::path::Path;
use std::time::Duration;

use super::proc::{BoundedError, run_bounded};

/// The bound on a `systemctl` invocation: a restart should return promptly; a wedged systemctl /
/// hung unit must not hang the upgrade forever (review fix #7). Generous, but bounded.
const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(30);

/// A typed service-manager failure.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// The manager tool (e.g. `systemctl`) could not be spawned at all.
    #[error("could not run {tool}: {detail}")]
    Spawn {
        /// The tool name.
        tool: String,
        /// Why the spawn failed.
        detail: String,
    },
    /// The manager tool ran but reported a non-zero status.
    #[error("{tool} failed (status {status}): {stderr}")]
    CommandFailed {
        /// The tool name.
        tool: String,
        /// The exit status (code or "a signal").
        status: String,
        /// The captured stderr (trimmed).
        stderr: String,
    },
    /// The manager tool did not exit within its timeout and was killed (review fix #7).
    #[error("{tool} did not exit within {timeout:?} (killed); the restart is in an unknown state")]
    Timeout {
        /// The tool name.
        tool: String,
        /// The deadline that elapsed.
        timeout: Duration,
    },
}

/// How the server process is restarted onto the swapped-in binary.
pub trait ServiceManager {
    /// Restart `unit`, returning once the manager reports the restart was issued/completed.
    ///
    /// # Errors
    ///
    /// Returns a [`ServiceError`] when the restart could not be issued.
    fn restart(&self, unit: &str) -> Result<(), ServiceError>;
}

/// The v1 manager: `systemctl restart <unit>`. With `Type=simple` + `Restart=on-failure` (the
/// shipped unit), `systemctl restart` stops the old process and starts the new one onto the
/// now-swapped binary; the post-restart health gate (not this call) decides success.
pub struct SystemdManager;

impl ServiceManager for SystemdManager {
    fn restart(&self, unit: &str) -> Result<(), ServiceError> {
        run_systemctl(&["restart", unit])
    }
}

/// Run `systemctl <args...>` BOUNDED by [`SYSTEMCTL_TIMEOUT`] and map the result to a typed error.
/// Captures stderr so a failure is reportable; a hung systemctl is killed and surfaced as
/// [`ServiceError::Timeout`] rather than hanging the upgrade (review fix #7).
fn run_systemctl(args: &[&str]) -> Result<(), ServiceError> {
    let output = match run_bounded(Path::new("systemctl"), args, SYSTEMCTL_TIMEOUT) {
        Ok(output) => output,
        Err(BoundedError::Timeout { timeout, .. }) => {
            return Err(ServiceError::Timeout {
                tool: "systemctl".to_owned(),
                timeout,
            });
        }
        // A spawn failure (systemctl not found) and a wait/IO failure both mean we could not run the
        // tool to completion; surface them as Spawn with their detail.
        Err(BoundedError::Spawn { detail, .. } | BoundedError::Wait { detail, .. }) => {
            return Err(ServiceError::Spawn {
                tool: "systemctl".to_owned(),
                detail,
            });
        }
    };
    if output.status.success() {
        Ok(())
    } else {
        Err(ServiceError::CommandFailed {
            tool: "systemctl".to_owned(),
            status: output
                .status
                .code()
                .map_or_else(|| "a signal".to_owned(), |c| c.to_string()),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// A mock that records the restarted units and can be scripted to fail. Demonstrates the seam is
    /// cleanly mockable (the orchestration tests use their own copy of this shape).
    struct RecordingManager {
        units: RefCell<Vec<String>>,
        fail: bool,
    }
    impl ServiceManager for RecordingManager {
        fn restart(&self, unit: &str) -> Result<(), ServiceError> {
            self.units.borrow_mut().push(unit.to_owned());
            if self.fail {
                Err(ServiceError::CommandFailed {
                    tool: "systemctl".to_owned(),
                    status: "1".to_owned(),
                    stderr: "Unit not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn mock_records_the_restarted_unit() {
        let m = RecordingManager {
            units: RefCell::new(Vec::new()),
            fail: false,
        };
        m.restart("ironcache").expect("ok");
        assert_eq!(m.units.borrow().as_slice(), ["ironcache"]);
    }

    #[test]
    fn mock_failure_is_typed() {
        let m = RecordingManager {
            units: RefCell::new(Vec::new()),
            fail: true,
        };
        let err = m.restart("ironcache").expect_err("scripted failure");
        assert!(matches!(err, ServiceError::CommandFailed { .. }), "{err:?}");
    }
}
