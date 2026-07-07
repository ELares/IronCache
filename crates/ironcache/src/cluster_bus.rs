// SPDX-License-Identifier: MIT OR Apache-2.0
//! Boot-time SECURITY warning for an UNAUTHENTICATED cluster bus (#557, M2 config safety).
//!
//! In a clustered mode the inter-node control plane (the RAFTMSG consensus bus at
//! [`ironcache::raft_boot::bus_port`], `port + 10000`) and the replication data-plane stream run
//! PLAINTEXT and UNAUTHENTICATED by DEFAULT: a `cluster_secret` (shared-secret peer authentication)
//! and/or `cluster_tls` are opt-in. Left unset, any party that can reach the bus port could join
//! consensus (forge RAFTMSG) or siphon the entire keyspace off the replication stream. Redis warns
//! loudly in the same spot; so do we.
//!
//! This is a LOUD `tracing::warn!` at boot, NOT a hard failure: the default clustered paths (and the
//! test suite that drives them) run without bus auth today, so hard-failing would break them without
//! an explicit opt-out. The pure posture decision lives on `Config::cluster_bus_unauthenticated`
//! (unit-tested there); this module is the emit seam (boot / OS side, outside the ADR-0003
//! determinism boundary, like `fd_budget`).

use ironcache::raft_boot::bus_port;
use ironcache_config::Config;

/// Emit a prominent boot warning when this node is configured for a clustered mode but its
/// inter-node bus / replication link is unauthenticated and unencrypted (no `cluster_secret`, and
/// `cluster_tls` off). A no-op for the default standalone node and for any authenticated posture
/// (a `cluster_secret`, or `cluster_tls = on` which requires one), so a properly-secured or
/// non-clustered boot logs nothing new.
pub fn warn_if_unauthenticated(cfg: &Config) {
    if !cfg.cluster_bus_unauthenticated() {
        return;
    }
    let bus = bus_port(cfg.port);
    tracing::warn!(
        cluster_bus_port = bus,
        "SECURITY: this node is configured for a CLUSTERED mode but the inter-node cluster bus is \
         UNAUTHENTICATED and UNENCRYPTED (no cluster_secret, cluster_tls off). The RAFTMSG \
         consensus bus (port {bus}) and the replication data-plane stream are PLAINTEXT and accept \
         ANY peer that can reach the port: an unauthorized party on the network could join \
         consensus / forge RAFTMSG or SIPHON the entire keyspace off the replication stream. \
         Secure it with cluster_secret (shared-secret peer authentication) and, to encrypt the \
         link, cluster_tls = on plus cluster_tls_cert_path / cluster_tls_key_path / cluster_ca_path \
         (see the cluster_secret / cluster_tls configuration docs)."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironcache_config::{ClusterMode, Config, TlsMode};
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    /// A `MakeWriter` that captures the subscriber's formatted output into a shared buffer, so a
    /// test can assert (via a tracing capture, the #543 pattern) that the warning actually fired.
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

    /// Run `warn_if_unauthenticated(cfg)` under a thread-local WARN-level capturing subscriber and
    /// return whatever it logged. `with_default` is thread-scoped, so parallel tests never race.
    fn captured_output(cfg: &Config) -> String {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CaptureWriter(Arc::clone(&buf)))
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || warn_if_unauthenticated(cfg));
        let bytes = buf.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn clustered_without_bus_auth_emits_prominent_warning() {
        let cfg = Config {
            cluster_enabled: true,
            ..Config::default()
        };
        let out = captured_output(&cfg);
        assert!(
            out.contains("WARN"),
            "a WARN-level event must fire, got: {out:?}"
        );
        assert!(
            out.contains("UNAUTHENTICATED"),
            "the warning must name the exposure, got: {out:?}"
        );
        assert!(
            out.contains(&bus_port(cfg.port).to_string()),
            "the warning must name the cluster-bus port, got: {out:?}"
        );
    }

    #[test]
    fn default_standalone_node_emits_no_warning() {
        // The default (cluster_enabled = false, cluster_mode = Static) boot is byte-unchanged.
        assert!(
            captured_output(&Config::default()).is_empty(),
            "a non-clustered node must not warn"
        );
    }

    #[test]
    fn authenticated_cluster_emits_no_warning() {
        // A shared secret authenticates the peer even on a plaintext bus -> no warning.
        let with_secret = Config {
            cluster_enabled: true,
            cluster_secret: Some("s3cret".to_owned()),
            ..Config::default()
        };
        assert!(captured_output(&with_secret).is_empty());

        // cluster_tls = on (which validate requires to carry a secret) is likewise authenticated.
        let with_tls = Config {
            cluster_mode: ClusterMode::Raft,
            cluster_tls: TlsMode::On,
            cluster_secret: Some("s3cret".to_owned()),
            ..Config::default()
        };
        assert!(captured_output(&with_tls).is_empty());
    }
}
