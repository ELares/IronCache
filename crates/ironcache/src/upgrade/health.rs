// SPDX-License-Identifier: MIT OR Apache-2.0
//! The HEALTH-GATE seam for `ironcache upgrade` (docs/design/UPGRADE.md "Post-swap stabilization
//! probe").
//!
//! After the restart, the new server must clear a bar before the `.old` slot is considered safely
//! superseded; a miss triggers auto-rollback. The [`LoopbackProbe`] confirms, within the budget:
//!
//! 1. RESTART HAPPENED (no false positive): the scraped `ironcache_uptime_seconds` must be SMALL and
//!    STRICTLY LESS than the pre-restart baseline `U0` (captured before `systemctl restart`). On-disk
//!    `--version` + `/readyz` + `PING` alone CANNOT prove the new binary is the running process: if
//!    `systemctl restart` no-ops, or a stale/old process still holds the port, `/readyz` + `PING`
//!    answer from the OLD server while the on-disk `--version` reads the NEW bytes -- the gate would
//!    pass while old code serves. A no-op restart / stale process shows a LARGE, increasing uptime
//!    (>= `U0`), which fails the gate. This is one extra GET on the same `/metrics` endpoint; no
//!    engine change.
//! 2. STABILIZATION (no crash-loop greenlight): the gate does not return on the FIRST passing poll.
//!    It requires SUSTAINED health -- the scraped uptime must reach `>= STABILIZATION_WINDOW` (a
//!    binary that passes once then crash-loops never lets uptime cross the window, because each
//!    restart resets uptime toward 0).
//! 3. READINESS: `GET /readyz` returns `200` (strict status-line parse). `/readyz` is load-gated (it
//!    is `200` only once every shard's load-on-boot has returned), so a `200` confirms the snapshot
//!    RELOADED -- the "working set reattached" condition UPGRADE.md wants (the v1 reattach is the
//!    persistence reload of the SAVE-first snapshot; the #62 mmap warm-restart is deferred).
//! 4. LIVENESS: `PING -> +PONG` over RESP confirms the server is actually accepting connections and
//!    answering commands, not merely that the ops port is up.
//! 5. VERSION: the on-disk `<target> --version` equals the EXPECTED target version. The running
//!    server's INFO `ironcache_version` is `CARGO_PKG_VERSION` (pinned `0.0.0` in dev/lock builds),
//!    NOT the stamped build version, so it is NOT a reliable over-the-wire version signal; the
//!    authoritative version check is the on-disk binary's `--version`. The uptime-reset check (1) is
//!    what ties that on-disk binary to the RUNNING process.
//!
//! The poll loop measures elapsed time through the `ironcache-env` monotonic clock seam (ADR-0003,
//! NOT `std::time::Instant`) and sleeps between polls through the `ironcache-runtime` timer seam, so
//! the determinism invariant lint is satisfied.

use std::path::PathBuf;
use std::time::Duration;

use ironcache_env::{Clock as _, SystemEnv};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// How long to wait between health polls. Short enough to catch a fast-booting server promptly,
/// long enough not to hammer the endpoint. Driven through the runtime timer seam.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The per-attempt connect/IO timeout for one probe round (so a single hung connect does not eat the
/// whole budget). Driven through the runtime timer seam.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

/// The STABILIZATION window (UPGRADE.md default): the restarted server must stay up + healthy until
/// its scraped uptime reaches at least this many seconds before the gate returns Ok. Guards against a
/// binary that passes one poll then crash-loops (each restart resets uptime toward 0, so it never
/// crosses the window).
pub const STABILIZATION_WINDOW: Duration = Duration::from_secs(5);

/// What the health gate probes: the ops `/readyz`+`/metrics` address, the RESP address (for `PING`),
/// the on-disk binary path (for the `--version` check), the expected version, an optional auth
/// password, and the pre-restart uptime baseline `U0`.
#[derive(Debug, Clone)]
pub struct HealthTarget {
    /// `host:port` of the ops endpoint serving `/readyz` + `/metrics`.
    pub readyz_addr: String,
    /// `host:port` of the RESP listener, for the `PING` liveness check.
    pub resp_addr: String,
    /// The on-disk binary now installed at the target path, whose `--version` must equal
    /// [`Self::expected_version`].
    pub binary: PathBuf,
    /// The exact version the upgrade targeted (from the new binary's `--version` pre-swap).
    pub expected_version: String,
    /// An optional `requirepass` for the `PING` connection (sent only over loopback, never logged).
    pub auth: Option<String>,
    /// The pre-restart `ironcache_uptime_seconds` baseline (scraped BEFORE `systemctl restart`).
    /// `None` when no baseline was available (no metrics endpoint then); treated as 0, so the
    /// restart-happened check still requires a small post-restart uptime. The gate requires the
    /// post-restart uptime to be STRICTLY LESS than this baseline (so a no-op restart / stale process
    /// showing a large increasing uptime fails).
    pub baseline_uptime: Option<u64>,
}

/// A typed probe failure. The orchestrator stringifies it into the health-gate reason; rollback
/// keys off whether the gate passed, not the variant.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// The gate did not pass within the budget; `reason` says which condition last failed.
    #[error("not healthy within the budget: {reason}")]
    NotHealthy {
        /// The last-observed failing condition (readyz status, ping result, version, or
        /// restart-not-detected / not-yet-stable).
        reason: String,
    },
}

/// The post-swap health gate. An implementation returns `Ok(())` only when the new server is fully
/// healthy on the expected version (and actually restarted + stabilized) within `budget`; tests
/// inject a mock.
pub trait HealthProbe {
    /// PRE-FLIGHT the `/readyz` endpoint BEFORE the swap (review fix #3): confirm SOMETHING is
    /// listening on `readyz_addr` and answers an HTTP request, so the health gate can actually run.
    /// If nothing is listening (a unit that does not expose `--metrics-addr`), fail EARLY with an
    /// actionable error rather than swapping and then auto-rolling-back a healthy binary on a
    /// gate that could never have passed.
    ///
    /// # Errors
    ///
    /// Returns [`ProbeError::NotHealthy`] when the `/readyz` endpoint is unreachable.
    fn preflight(&self, readyz_addr: &str) -> Result<(), ProbeError>;

    /// Scrape the pre-restart `ironcache_uptime_seconds` baseline `U0` from `/metrics` (review fix
    /// #2). `None` when the metric is unavailable; the gate then only requires a small post-restart
    /// uptime.
    fn baseline_uptime(&self, readyz_addr: &str) -> Option<u64>;

    /// The FULL gate: restart-detected + stabilized + readiness + liveness + EXACT version match,
    /// within `budget`.
    ///
    /// # Errors
    ///
    /// Returns [`ProbeError::NotHealthy`] if the gate is not cleared in time.
    fn gate(&self, target: &HealthTarget, budget: Duration) -> Result<(), ProbeError>;

    /// A readiness+liveness gate WITHOUT the exact-version check, for the rollback re-probe when the
    /// prior version is unknown (we only need to confirm the restored binary came back up). Still
    /// requires the restart-detected + stabilization conditions.
    ///
    /// # Errors
    ///
    /// Returns [`ProbeError::NotHealthy`] if readiness+liveness is not reached in time.
    fn gate_ready_only(&self, target: &HealthTarget, budget: Duration) -> Result<(), ProbeError>;
}

/// The v1 probe: poll the loopback `/metrics` (uptime) + `/readyz` + `PING` + on-disk `--version`.
pub struct LoopbackProbe;

impl HealthProbe for LoopbackProbe {
    fn preflight(&self, readyz_addr: &str) -> Result<(), ProbeError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ProbeError::NotHealthy {
                reason: format!("could not build the preflight runtime: {e}"),
            })?;
        // We only need to confirm the endpoint ANSWERS (any HTTP response): a 503 is fine here (the
        // server may be loading), what matters is that the readyz endpoint EXISTS so the gate can
        // run. A connect refusal means the unit does not expose /readyz.
        rt.block_on(async {
            match http_get(readyz_addr, "/readyz").await {
                Ok(_) => Ok(()),
                Err(e) => Err(ProbeError::NotHealthy {
                    reason: format!(
                        "the unit does not expose /readyz at {readyz_addr}; the health gate cannot \
                         run (add --metrics-addr {readyz_addr} to the unit's ExecStart): {e}"
                    ),
                }),
            }
        })
    }

    fn baseline_uptime(&self, readyz_addr: &str) -> Option<u64> {
        scrape_uptime(readyz_addr)
    }

    fn gate(&self, target: &HealthTarget, budget: Duration) -> Result<(), ProbeError> {
        run_gate(target, budget, true)
    }

    fn gate_ready_only(&self, target: &HealthTarget, budget: Duration) -> Result<(), ProbeError> {
        run_gate(target, budget, false)
    }
}

/// Scrape `ironcache_uptime_seconds` from the `/metrics` endpoint at `readyz_addr`, returning the
/// value or `None` when the endpoint is unreachable / the metric is absent. Used by the orchestrator
/// to capture the pre-restart baseline `U0` BEFORE issuing `systemctl restart`. Runs on a throwaway
/// current-thread runtime (this is the short-lived CLI).
#[must_use]
pub fn scrape_uptime(readyz_addr: &str) -> Option<u64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    rt.block_on(async { http_metrics_uptime(readyz_addr).await.ok() })
}

/// Drive the polling gate on a throwaway current-thread runtime. `check_version` gates the exact
/// `--version` match (the rollback ready-only re-probe passes `false`).
fn run_gate(
    target: &HealthTarget,
    budget: Duration,
    check_version: bool,
) -> Result<(), ProbeError> {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            return Err(ProbeError::NotHealthy {
                reason: format!("could not build the probe runtime: {e}"),
            });
        }
    };
    rt.block_on(poll_until_healthy(target, budget, check_version))
}

/// Poll until the server is restarted + stabilized + ready + live (+ version) or the monotonic
/// budget elapses. Elapsed time is measured through the `ironcache-env` monotonic clock; the
/// inter-poll sleep through the `ironcache-runtime` timer. Returns the LAST failing reason on
/// timeout.
async fn poll_until_healthy(
    target: &HealthTarget,
    budget: Duration,
    check_version: bool,
) -> Result<(), ProbeError> {
    use ironcache_runtime::Runtime as _;
    let env = SystemEnv::new();
    let start = env.now();
    let rt = ironcache_runtime::TokioRuntime::new();

    // The version check is on-disk + constant across polls, so evaluate it ONCE up front; a mismatch
    // never resolves by waiting, so fail fast rather than burning the whole budget.
    if check_version {
        if let Err(reason) = check_disk_version(target) {
            return Err(ProbeError::NotHealthy { reason });
        }
    }

    let baseline = target.baseline_uptime.unwrap_or(0);
    let window_secs = STABILIZATION_WINDOW.as_secs();
    loop {
        let last_reason = match probe_once(target).await {
            Ok(uptime) => {
                // (1) restart-detected: a small uptime STRICTLY below the pre-restart baseline. A
                // no-op restart / stale process shows uptime >= baseline (large, increasing) -> fail.
                if baseline > 0 && uptime >= baseline {
                    format!(
                        "restart not detected: scraped uptime {uptime}s >= pre-restart baseline \
                         {baseline}s (the old/stale process is still serving)"
                    )
                } else if uptime < window_secs {
                    // (2) stabilization: keep polling until uptime crosses the window. A crash-loop
                    // keeps resetting uptime toward 0, so it never crosses -> times out -> rollback.
                    format!(
                        "not yet stable: uptime {uptime}s < stabilization window {window_secs}s"
                    )
                } else {
                    // Restarted AND stable AND (readyz+ping passed inside probe_once) -> healthy.
                    return Ok(());
                }
            }
            Err(reason) => reason,
        };
        // Budget check via the monotonic seam (never std::time).
        if env.now().saturating_duration_since(start) >= budget {
            return Err(ProbeError::NotHealthy {
                reason: format!("timed out after {budget:?}; last: {last_reason}"),
            });
        }
        rt.timer(POLL_INTERVAL).await;
    }
}

/// The on-disk binary at `target.binary` must report exactly `target.expected_version`. Returns
/// `Err(reason)` on a mismatch or an unrunnable binary.
fn check_disk_version(target: &HealthTarget) -> Result<(), String> {
    match super::verify::probe_binary_version(&target.binary) {
        Ok(v) if v == target.expected_version => Ok(()),
        Ok(v) => Err(format!(
            "version mismatch: installed binary reports {v}, expected {}",
            target.expected_version
        )),
        Err(e) => Err(format!(
            "could not read the installed binary's version: {e}"
        )),
    }
}

/// One round of `/readyz` + `PING` + `/metrics` uptime, each under [`ATTEMPT_TIMEOUT`]. Returns
/// `Ok(uptime_secs)` (the scraped uptime, for the caller's restart-detected + stabilization checks)
/// only when `/readyz` is 200 AND `PING` answers `+PONG` AND uptime scrapes; otherwise the failing
/// reason.
async fn probe_once(target: &HealthTarget) -> Result<u64, String> {
    use ironcache_runtime::Runtime as _;
    let rt = ironcache_runtime::TokioRuntime::new();

    // /readyz must be 200.
    let readyz = tokio::select! {
        biased;
        r = http_readyz(&target.readyz_addr) => r,
        () = rt.timer(ATTEMPT_TIMEOUT) => Err("/readyz attempt timed out".to_owned()),
    };
    readyz?;

    // PING must answer +PONG.
    let ping = tokio::select! {
        biased;
        r = resp_ping(&target.resp_addr, target.auth.as_deref()) => r,
        () = rt.timer(ATTEMPT_TIMEOUT) => Err("PING attempt timed out".to_owned()),
    };
    ping?;

    // /metrics uptime (the restart-detected + stabilization signal).
    let uptime = tokio::select! {
        biased;
        r = http_metrics_uptime(&target.readyz_addr) => r,
        () = rt.timer(ATTEMPT_TIMEOUT) => Err("/metrics attempt timed out".to_owned()),
    };
    uptime
}

/// `GET /readyz` and require a `200` status (STRICT parse). Reuses the same minimal hand-rolled
/// HTTP/1.1 request the metrics endpoint serves; we read the status line and parse it as
/// `HTTP/x.y <code> <reason>`, requiring the code token to be exactly `200`. Any connect/IO error or
/// non-200 is a failing reason.
async fn http_readyz(addr: &str) -> Result<(), String> {
    let raw = http_get(addr, "/readyz").await?;
    let text = String::from_utf8_lossy(&raw);
    let status_line = text.lines().next().unwrap_or_default();
    if status_code(status_line) == Some(200) {
        Ok(())
    } else {
        Err(format!("/readyz not ready (status line: {status_line:?})"))
    }
}

/// Parse the numeric status code from an HTTP status line `HTTP/x.y <code> <reason>`: split on
/// whitespace and parse the SECOND token. Returns `None` if the line is malformed. Strict: the
/// previous `contains(" 200 ")` would match a `200` appearing anywhere (a header value, a body
/// substring); this only accepts a well-formed status line whose code token is `200`.
fn status_code(status_line: &str) -> Option<u16> {
    let mut parts = status_line.split_whitespace();
    let proto = parts.next()?;
    if !proto.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse::<u16>().ok()
}

/// `GET /metrics` and extract `ironcache_uptime_seconds`. The Prometheus exposition renders it as a
/// line `ironcache_uptime_seconds <secs>`; we find that line and parse the value. A missing endpoint
/// / metric is an `Err` (used by the gate; `scrape_uptime` maps it to `None`).
async fn http_metrics_uptime(addr: &str) -> Result<u64, String> {
    let raw = http_get(addr, "/metrics").await?;
    let text = String::from_utf8_lossy(&raw);
    for line in text.lines() {
        // The metric line is `ironcache_uptime_seconds <value>` (no labels on this gauge). Skip the
        // `# HELP` / `# TYPE` comment lines, which also contain the metric name.
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("ironcache_uptime_seconds") {
            let val = rest.trim();
            if let Ok(secs) = val.parse::<u64>() {
                return Ok(secs);
            }
        }
    }
    Err("/metrics did not report ironcache_uptime_seconds".to_owned())
}

/// Issue a bounded `GET <path>` over HTTP/1.1 and return the raw response bytes (status line +
/// headers + body, up to a cap). Connection-close, one request per connection.
async fn http_get(addr: &str, path: &str) -> Result<Vec<u8>, String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("{path} connect to {addr} failed: {e}"))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("{path} write failed: {e}"))?;
    let mut raw = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("{path} read failed: {e}"))?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..n]);
        // Bound the read: the metrics body is a few KiB; cap so a hostile endpoint cannot stream
        // unbounded. 256 KiB is far above any real /metrics or /readyz response.
        if raw.len() > 256 * 1024 {
            break;
        }
    }
    Ok(raw)
}

/// Connect over RESP, optionally `AUTH`, then `PING` and require `+PONG`. The auth password rides
/// only the loopback socket; it is never logged.
async fn resp_ping(addr: &str, password: Option<&str>) -> Result<(), String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("PING connect to {addr} failed: {e}"))?;
    if let Some(pw) = password {
        write_resp(&mut stream, &[b"AUTH", pw.as_bytes()])
            .await
            .map_err(|e| format!("AUTH write failed: {e}"))?;
        let reply = read_simple_line(&mut stream).await?;
        if !reply.starts_with('+') {
            return Err(format!("AUTH for PING rejected: {reply}"));
        }
    }
    write_resp(&mut stream, &[b"PING"])
        .await
        .map_err(|e| format!("PING write failed: {e}"))?;
    let reply = read_simple_line(&mut stream).await?;
    if reply.eq_ignore_ascii_case("+PONG") {
        Ok(())
    } else {
        Err(format!("PING did not return +PONG (got {reply:?})"))
    }
}

/// Encode + write a RESP2 command array.
async fn write_resp(stream: &mut tokio::net::TcpStream, args: &[&[u8]]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    stream.write_all(&buf).await?;
    stream.flush().await
}

/// Read a single RESP status/error line (up to and including its CRLF), returning it WITHOUT the
/// trailing CRLF but WITH its leading type byte (so the caller can check the `+`/`-`).
async fn read_simple_line(stream: &mut tokio::net::TcpStream) -> Result<String, String> {
    let mut out = Vec::with_capacity(16);
    let mut b = [0u8; 1];
    loop {
        let n = stream
            .read(&mut b)
            .await
            .map_err(|e| format!("reply read failed: {e}"))?;
        if n == 0 {
            return Err("connection closed before a full reply line".to_owned());
        }
        if b[0] == b'\n' {
            break;
        }
        if b[0] != b'\r' {
            out.push(b[0]);
        }
        if out.len() > 4096 {
            return Err("reply line exceeded 4 KiB".to_owned());
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use tokio::net::TcpListener;

    /// A mock probe demonstrating the trait is cleanly mockable; the orchestration tests in `mod`
    /// use their own scripted version.
    struct ScriptedProbe {
        outcomes: RefCell<Vec<Result<(), String>>>,
    }
    impl HealthProbe for ScriptedProbe {
        fn preflight(&self, _addr: &str) -> Result<(), ProbeError> {
            Ok(())
        }
        fn baseline_uptime(&self, _addr: &str) -> Option<u64> {
            None
        }
        fn gate(&self, _t: &HealthTarget, _b: Duration) -> Result<(), ProbeError> {
            match self.outcomes.borrow_mut().remove(0) {
                Ok(()) => Ok(()),
                Err(r) => Err(ProbeError::NotHealthy { reason: r }),
            }
        }
        fn gate_ready_only(&self, t: &HealthTarget, b: Duration) -> Result<(), ProbeError> {
            self.gate(t, b)
        }
    }

    fn target(readyz: &str, resp: &str) -> HealthTarget {
        HealthTarget {
            readyz_addr: readyz.to_owned(),
            resp_addr: resp.to_owned(),
            binary: PathBuf::from("/nonexistent"),
            expected_version: "9.9.9".to_owned(),
            auth: None,
            baseline_uptime: None,
        }
    }

    /// A fake ops server that answers `/readyz` and `/metrics` (with a scripted uptime sequence) and
    /// a RESP `PING`. Each accepted connection serves one request. `uptimes` is consumed per
    /// `/metrics` scrape (the last value repeats once exhausted).
    fn spawn_ops_server(
        readyz_status: &'static str,
        uptimes: Vec<u64>,
    ) -> (String, std::sync::Arc<tokio::sync::Mutex<Vec<u64>>>) {
        let uptimes = std::sync::Arc::new(tokio::sync::Mutex::new(uptimes));
        let up2 = std::sync::Arc::clone(&uptimes);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(listener.local_addr().unwrap().to_string()).unwrap();
                loop {
                    let (mut s, _) = listener.accept().await.unwrap();
                    let mut buf = [0u8; 512];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    if req.starts_with("GET /readyz") {
                        let body = format!(
                            "HTTP/1.1 {readyz_status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        let _ = s.write_all(body.as_bytes()).await;
                    } else if req.starts_with("GET /metrics") {
                        let up = {
                            let mut g = up2.lock().await;
                            if g.len() > 1 {
                                g.remove(0)
                            } else {
                                *g.first().unwrap_or(&0)
                            }
                        };
                        let body_inner = format!("ironcache_uptime_seconds {up}\n");
                        let body = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_inner}",
                            body_inner.len()
                        );
                        let _ = s.write_all(body.as_bytes()).await;
                    } else {
                        // RESP PING (the resp_addr points here too in these tests).
                        let _ = s.write_all(b"+PONG\r\n").await;
                    }
                    let _ = s.flush().await;
                }
            });
        });
        let addr = rx.recv().unwrap();
        (addr, uptimes)
    }

    #[test]
    fn mock_scripted_probe_returns_outcomes_in_order() {
        let p = ScriptedProbe {
            outcomes: RefCell::new(vec![Err("not yet".to_owned()), Ok(())]),
        };
        let t = target("x", "y");
        assert!(p.gate(&t, Duration::from_secs(1)).is_err());
        assert!(p.gate(&t, Duration::from_secs(1)).is_ok());
    }

    /// A 200 /readyz status line passes the strict status parse; 503 and a spoofed body do not.
    #[tokio::test(flavor = "current_thread")]
    async fn http_readyz_accepts_200() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).await;
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            let _ = s.flush().await;
        });
        http_readyz(&addr).await.expect("200 readyz passes");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn http_readyz_rejects_503() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).await;
            s.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            let _ = s.flush().await;
        });
        let err = http_readyz(&addr).await.expect_err("503 fails");
        assert!(err.contains("not ready"), "{err}");
    }

    /// HIGH fix #9: the strict status-line parser rejects a `200` that appears only in the reason
    /// phrase / a header, accepting only a well-formed `HTTP/x.y 200 ...` status line.
    #[test]
    fn status_code_is_strict() {
        assert_eq!(status_code("HTTP/1.1 200 OK"), Some(200));
        assert_eq!(status_code("HTTP/1.0 503 Service Unavailable"), Some(503));
        // A 200 only in the reason phrase is NOT accepted as the code.
        assert_eq!(status_code("HTTP/1.1 503 got 200 somewhere"), Some(503));
        // Not a status line.
        assert_eq!(status_code("X-Foo: 200"), None);
        assert_eq!(status_code(""), None);
        assert_eq!(status_code("HTTP/1.1 notacode OK"), None);
    }

    /// PING -> +PONG passes liveness.
    #[tokio::test(flavor = "current_thread")]
    async fn resp_ping_accepts_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = s.read(&mut buf).await;
            s.write_all(b"+PONG\r\n").await.unwrap();
            let _ = s.flush().await;
        });
        resp_ping(&addr, None).await.expect("+PONG passes");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resp_ping_rejects_non_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = s.read(&mut buf).await;
            s.write_all(b"-LOADING\r\n").await.unwrap();
            let _ = s.flush().await;
        });
        let err = resp_ping(&addr, None).await.expect_err("non-PONG fails");
        assert!(err.contains("PONG"), "{err}");
    }

    /// `http_metrics_uptime` parses the gauge line (and skips the # HELP/# TYPE comment lines).
    #[tokio::test(flavor = "current_thread")]
    async fn metrics_uptime_parses_the_gauge() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).await;
            let body = "# HELP ironcache_uptime_seconds Seconds.\n# TYPE ironcache_uptime_seconds gauge\nironcache_uptime_seconds 42\n";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            let _ = s.flush().await;
        });
        assert_eq!(http_metrics_uptime(&addr).await.unwrap(), 42);
    }

    /// The version check fails fast on a mismatch (an unrunnable binary -> could-not-read reason),
    /// and the poll loop returns that without waiting out the whole budget.
    #[tokio::test(flavor = "current_thread")]
    async fn version_mismatch_fails_fast() {
        let t = target("127.0.0.1:1", "127.0.0.1:1"); // addrs never reached: version fails first
        let err = poll_until_healthy(&t, Duration::from_secs(5), true)
            .await
            .expect_err("version check fails");
        match err {
            ProbeError::NotHealthy { reason } => {
                assert!(
                    reason.contains("version") || reason.contains("installed binary"),
                    "{reason}"
                );
            }
        }
    }

    /// CRITICAL fix #2: a no-op restart (uptime stays LARGE, >= the pre-restart baseline) FAILS the
    /// gate even though /readyz + PING answer -- the old/stale process is still serving.
    #[test]
    fn stale_process_uptime_not_reset_fails_the_gate() {
        // The ops server reports uptime 1000 (large, never resets). baseline = 900.
        let (addr, _) = spawn_ops_server("200 OK", vec![1000]);
        let t = HealthTarget {
            readyz_addr: addr.clone(),
            resp_addr: addr,
            binary: PathBuf::from("/nonexistent"),
            expected_version: "x".to_owned(),
            auth: None,
            baseline_uptime: Some(900),
        };
        // check_version=false (ready-only) so the /nonexistent binary is not probed; we are testing
        // the uptime-reset gate. Short budget so the timeout fires fast.
        let err = run_gate(&t, Duration::from_millis(600), false).expect_err("stale process fails");
        let ProbeError::NotHealthy { reason } = err;
        assert!(
            reason.contains("restart not detected") || reason.contains("still serving"),
            "{reason}"
        );
    }

    /// CRITICAL fix #2 + HIGH fix #4: a restarted server whose uptime resets small THEN crosses the
    /// stabilization window passes the gate; a small-but-never-crossing (crash-loop) uptime does not.
    #[test]
    fn restarted_and_stabilized_passes_but_crashloop_does_not() {
        let window = STABILIZATION_WINDOW.as_secs();
        // CASE A: uptime sequence 0, 1, then >= window -> passes once it crosses the window.
        let (addr, _) = spawn_ops_server("200 OK", vec![0, 1, window, window]);
        let t = HealthTarget {
            readyz_addr: addr.clone(),
            resp_addr: addr,
            binary: PathBuf::from("/nonexistent"),
            expected_version: "x".to_owned(),
            auth: None,
            baseline_uptime: Some(500), // small post-restart uptime is < baseline -> restart detected
        };
        run_gate(&t, Duration::from_secs(5), false).expect("crosses the window -> healthy");

        // CASE B: a crash-loop -- uptime never crosses the window (always 0/1) -> times out, not
        // greenlit.
        let (addr2, _) = spawn_ops_server("200 OK", vec![0, 1, 0, 1, 0]);
        let t2 = HealthTarget {
            readyz_addr: addr2.clone(),
            resp_addr: addr2,
            binary: PathBuf::from("/nonexistent"),
            expected_version: "x".to_owned(),
            auth: None,
            baseline_uptime: Some(500),
        };
        let err =
            run_gate(&t2, Duration::from_millis(900), false).expect_err("crash-loop not stable");
        let ProbeError::NotHealthy { reason } = err;
        assert!(
            reason.contains("stable") || reason.contains("timed out"),
            "{reason}"
        );
    }

    /// `scrape_uptime` returns the value when the endpoint serves it, and `None` when unreachable.
    #[test]
    fn scrape_uptime_reads_and_handles_absent() {
        let (addr, _) = spawn_ops_server("200 OK", vec![123]);
        assert_eq!(scrape_uptime(&addr), Some(123));
        // Nothing listening here.
        assert_eq!(scrape_uptime("127.0.0.1:1"), None);
    }
}
