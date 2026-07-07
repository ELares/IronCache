// SPDX-License-Identifier: MIT OR Apache-2.0
//! Loud boot log for systemd socket-activation adopt-vs-fallback (#562, M2 upgrade lifecycle).
//!
//! When the server boots it either ADOPTS the listening socket(s) systemd passed it (socket
//! activation, `LISTEN_FDS` / `LISTEN_PID`, docs/design/UPGRADE.md, #389) or FALLS BACK to binding
//! its own listener. Historically that choice was SILENT, so an operator could not tell from the
//! logs which path a socket-activated upgrade took -- exactly what is needed to debug a failed one
//! (e.g. a `LISTEN_PID` mismatch that quietly downgraded activation to a self-bind). This emits a
//! prominent one-line `tracing` event naming the path, the adopted fds (via `LISTEN_FDNAMES`), and,
//! on a fallback, WHY.
//!
//! The adopt-vs-fallback CLASSIFICATION is the pure [`ironcache_runtime::listen_fds::classify`]
//! (unit-tested there, over the SAME env parse the runtime's `listener_for` acts on, so the logged
//! decision matches the one taken); this module is only the emit seam (boot / OS side, outside the
//! ADR-0003 determinism boundary, like `cluster_bus` and `fd_budget`).

use ironcache_runtime::listen_fds::{Activation, SelfBindReason};

/// Emit the loud boot log for the socket-activation decision (#562).
///
/// INFO for the two expected outcomes -- adopting the passed fds, or a plain non-activated self-bind
/// -- and WARN for a `LISTEN_*` environment that was PRESENT but rejected (a foreign/missing
/// `LISTEN_PID`, a malformed count): that is a real misconfiguration silently downgrading a
/// socket-activated upgrade to a self-bind, so it deserves the louder level rather than hiding at
/// info.
pub fn log_socket_activation(activation: &Activation) {
    let summary = activation.boot_summary();
    match activation {
        Activation::Adopted(_) | Activation::SelfBound(SelfBindReason::NotActivated) => {
            tracing::info!("socket-activation: {summary}");
        }
        Activation::SelfBound(SelfBindReason::Rejected(_)) => {
            tracing::warn!("socket-activation: {summary}");
        }
    }
}

/// Read this process's socket-activation environment, classify it, and emit the boot log. The thin
/// wrapper `cmd_server` calls at boot; the classification + emit are split out (above) so both are
/// testable without a live systemd host.
pub fn log_boot_socket_activation() {
    let parsed = ironcache_runtime::listen_fds::from_env();
    log_socket_activation(&ironcache_runtime::listen_fds::classify(&parsed));
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_runtime::listen_fds::{ListenFdsError, parse_listen_fds};
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    /// A `MakeWriter` that captures the subscriber's formatted output into a shared buffer, so a
    /// test can assert (via a tracing capture, the #557 cluster_bus pattern) that the boot log
    /// actually fired on the branch under test.
    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `log_socket_activation` under a thread-local INFO-level capturing subscriber and return
    /// what it logged. `with_default` is thread-scoped, so parallel tests never race.
    fn captured(activation: &Activation) -> String {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CaptureWriter(Arc::clone(&buf)))
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || log_socket_activation(activation));
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn adopt_path_logs_info_naming_the_fds() {
        let activation = ironcache_runtime::listen_fds::classify(&parse_listen_fds(
            Some(&std::process::id().to_string()),
            Some("2"),
            Some("resp:repl"),
            std::process::id(),
        ));
        let out = captured(&activation);
        assert!(out.contains("INFO"), "adopt logs at INFO, got: {out:?}");
        assert!(
            out.contains("ADOPTED 2"),
            "names the adopt path + count: {out:?}"
        );
        assert!(out.contains("resp=fd3"), "names the RESP fd: {out:?}");
    }

    #[test]
    fn fallback_not_activated_logs_info() {
        let activation =
            ironcache_runtime::listen_fds::classify(&parse_listen_fds(None, None, None, 4242));
        let out = captured(&activation);
        assert!(
            out.contains("INFO"),
            "a normal self-bind logs at INFO: {out:?}"
        );
        assert!(
            out.contains("FELL BACK"),
            "names the fallback path: {out:?}"
        );
        assert!(out.contains("no LISTEN_FDS"), "states WHY: {out:?}");
    }

    #[test]
    fn rejected_env_logs_warn_naming_the_reason() {
        // A foreign LISTEN_PID: a real misconfig that must not hide at INFO.
        let activation =
            Activation::SelfBound(SelfBindReason::Rejected(ListenFdsError::PidMismatch {
                listen_pid: 9999,
                self_pid: 4242,
            }));
        let out = captured(&activation);
        assert!(
            out.contains("WARN"),
            "a rejected env logs at WARN, got: {out:?}"
        );
        assert!(out.contains("REJECTED"), "states it was rejected: {out:?}");
        assert!(out.contains("9999"), "names the foreign pid: {out:?}");
    }
}
